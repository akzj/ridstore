package ridstore

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/segment"
)

func TestActiveDataSyscallErrorsFailClosedAndRecoverOutcome(t *testing.T) {
	for _, test := range []struct {
		point     failpoint.Point
		committed bool
		unknown   bool
	}{
		{point: segment.PointBeforeAppendWrite},
		{point: segment.PointBeforeSync, committed: true, unknown: true},
	} {
		test := test
		t.Run(string(test.point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			var armed atomic.Bool
			hook := failpoint.Func(func(point failpoint.Point) error {
				if armed.Load() && point == test.point {
					return syscall.EIO
				}
				return nil
			})
			cfg := smallTestConfig(dir)
			store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Put(context.Background(), 1, []byte("value")); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			_, commitErr := batch.Commit(context.Background())
			if !errors.Is(commitErr, syscall.EIO) || errors.Is(commitErr, ErrCommitUnknown) != test.unknown {
				t.Fatalf("Commit error=%v", commitErr)
			}
			if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("write after syscall error=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			value, getErr := store.Get(context.Background(), 1)
			if test.committed {
				if getErr != nil || string(value) != "value" {
					t.Fatalf("recovered value=%q error=%v", value, getErr)
				}
			} else if !errors.Is(getErr, ErrNotFound) {
				t.Fatalf("uncommitted Get error=%v", getErr)
			}
			status, err := store.Status(context.Background(), batch.ID())
			if err != nil {
				t.Fatal(err)
			}
			want := BatchStateAborted
			if test.committed {
				want = BatchStateCommitted
			}
			if status.State != want {
				t.Fatalf("recovered status=%+v want=%v", status, want)
			}
		})
	}
}

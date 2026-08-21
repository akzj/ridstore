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
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/verify"
)

func TestCheckpointManifestSyscallErrorRequiresFreshOpen(t *testing.T) {
	for _, test := range []struct {
		point               failpoint.Point
		recoveredGeneration uint64
	}{
		{manifest.PointBeforeCurrentRename, 1},
		{manifest.PointBeforeRootDirSync, 2},
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
			store, err := createWithOptions(smallTestConfig(dir), initialize.Options{Hook: hook})
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			id, err := batch.Allocate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Put(context.Background(), id, []byte("durable")); err != nil {
				t.Fatal(err)
			}
			if _, err := batch.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}

			armed.Store(true)
			if err := store.Checkpoint(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("checkpoint error=%v", err)
			}
			if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("write after uncertain publication error=%v", err)
			}
			if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrReadOnly) {
				t.Fatalf("read after uncertain publication error=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = Open(smallTestConfig(dir))
			if err != nil {
				t.Fatal(err)
			}
			if generation := store.catalog.Snapshot().Generation; generation != test.recoveredGeneration {
				t.Fatalf("recovered generation=%d want=%d", generation, test.recoveredGeneration)
			}
			if value, err := store.Get(context.Background(), id); err != nil || string(value) != "durable" {
				t.Fatalf("recovered value=%q error=%v", value, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			report, err := verify.Run(context.Background(), dir)
			if err != nil || !report.Clean {
				t.Fatalf("verify=%+v error=%v", report, err)
			}
		})
	}
}

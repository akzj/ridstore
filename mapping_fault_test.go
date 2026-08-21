package ridstore

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/verify"
)

func TestActiveMappingSyscallErrorsFailClosedAndReplay(t *testing.T) {
	points := []failpoint.Point{radix.PointBeforeAppendWrite, radix.PointBeforeSync}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				cfg := smallTestConfig(dir)
				store, err := Create(cfg)
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
				if err := batch.Put(context.Background(), id, []byte("mapping-replay")); err != nil {
					t.Fatal(err)
				}
				committed, err := batch.Commit(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				store.mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				}))
				if err := store.Checkpoint(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("Checkpoint error=%v", err)
				}
				if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, cause.err) {
					t.Fatalf("write after Mapping error=%v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := Open(cfg)
				if err != nil {
					t.Fatalf("fresh Open: %v", err)
				}
				record, err := recovered.GetRecord(context.Background(), id)
				if err != nil || record.Revision != Revision(committed.BatchID) || string(record.Value) != "mapping-replay" {
					t.Fatalf("record=%+v error=%v", record, err)
				}
				if err := recovered.Checkpoint(context.Background()); err != nil {
					t.Fatalf("checkpoint after recovery: %v", err)
				}
				if err := recovered.Close(); err != nil {
					t.Fatal(err)
				}
				report, err := verify.Run(context.Background(), dir)
				if err != nil || !report.Clean {
					t.Fatalf("verify=%+v error=%v", report, err)
				}
			})
		}
	}
}

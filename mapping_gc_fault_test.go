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

func TestMappingGCSyscallErrorsFailClosedAndRecover(t *testing.T) {
	points := []failpoint.Point{
		radix.PointBeforeMappingGCHeaderWrite,
		radix.PointBeforeMappingGCPublishDirSync,
		radix.PointBeforeMappingGCTrashDeleteDirSync,
	}
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
				if err := batch.Put(context.Background(), id, []byte("mapping-gc-recovery")); err != nil {
					t.Fatal(err)
				}
				if _, err := batch.Commit(context.Background()); err != nil {
					t.Fatal(err)
				}
				store.mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				}))
				if err := store.CompactMapping(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("CompactMapping error=%v", err)
				}
				if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, cause.err) {
					t.Fatalf("write after Mapping GC error=%v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := Open(cfg)
				if err != nil {
					t.Fatalf("fresh Open: %v", err)
				}
				value, err := recovered.Get(context.Background(), id)
				if err != nil || string(value) != "mapping-gc-recovery" {
					t.Fatalf("value=%q error=%v", value, err)
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

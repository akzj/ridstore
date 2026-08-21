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

func TestMappingRotationSyscallErrorsFailClosedAndRecover(t *testing.T) {
	points := []failpoint.Point{
		radix.PointBeforeRotationActiveSync,
		radix.PointBeforeRotationFooterWrite,
		radix.PointBeforeRotationFooterSync,
		radix.PointBeforeRotationRename,
		radix.PointBeforeRotationDirSync,
		radix.PointBeforeRotationHeaderWrite,
		radix.PointBeforeRotationHeaderSync,
		radix.PointBeforeRotationCreateSync,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
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
			if err := batch.Put(context.Background(), id, []byte("before-rotation")); err != nil {
				t.Fatal(err)
			}
			if _, err := batch.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			fillActiveMappingForNestedDataGC(t, store, cfg)
			batch, err = store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Put(context.Background(), id, []byte("after-rotation")); err != nil {
				t.Fatal(err)
			}
			committed, err := batch.Commit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			store.mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return syscall.EIO
				}
				return nil
			}))
			if err := store.Checkpoint(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Checkpoint error=%v", err)
			}
			if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("write after rotation error=%v", err)
			}
			_ = store.Close()
			recovered, err := Open(cfg)
			if err != nil {
				t.Fatalf("fresh Open: %v", err)
			}
			record, err := recovered.GetRecord(context.Background(), id)
			if err != nil || record.Revision != Revision(committed.BatchID) || string(record.Value) != "after-rotation" {
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

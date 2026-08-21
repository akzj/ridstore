package ridstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/rotation"
	"github.com/akzj/ridstore/internal/segment"
	"github.com/akzj/ridstore/internal/verify"
)

func TestActiveDataCreateSyscallErrorsFailClosedAndRecover(t *testing.T) {
	points := []failpoint.Point{
		segment.PointBeforeCreateHeaderWrite,
		segment.PointBeforeCreateFileSync,
		segment.PointBeforeCreateDirSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range activeDataCreateCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				exerciseRotationFaultRecovery(t, errorAtActiveDataPoint(point, cause.err), cause.err)
			})
		}
	}
}

func TestActiveDataCreateRecoverySyscallErrorsAreRetryable(t *testing.T) {
	cases := []struct {
		name     string
		prepare  failpoint.Point
		recovery []failpoint.Point
	}{
		{
			name:    "create-missing",
			prepare: rotation.PointOldSealed,
			recovery: []failpoint.Point{
				segment.PointBeforeCreateHeaderWrite,
				segment.PointBeforeCreateFileSync,
				segment.PointBeforeCreateDirSync,
			},
		},
		{
			name:    "resync-existing",
			prepare: rotation.PointNewCreated,
			recovery: []failpoint.Point{
				segment.PointBeforeCreateFileSync,
				segment.PointBeforeCreateDirSync,
			},
		},
		{
			name:    "replace-partial",
			prepare: segment.PointBeforeCreateHeaderWrite,
			recovery: []failpoint.Point{
				rotation.PointBeforeNewActiveRemove,
				rotation.PointBeforeNewActiveRemoveSync,
			},
		},
	}
	for _, test := range cases {
		test := test
		for _, point := range test.recovery {
			point := point
			for _, cause := range activeDataCreateCauses() {
				cause := cause
				t.Run(test.name+"/"+string(point)+"/"+cause.name, func(t *testing.T) {
					cfg := prepareRotationRecovery(t, test.prepare)
					if _, err := openWithHook(cfg, errorAtActiveDataPoint(point, cause.err)); !errors.Is(err, cause.err) {
						t.Fatalf("recovery error=%v", err)
					}
					assertRotationRecoveryWritable(t, cfg)
				})
			}
		}
	}
}

func TestActiveDataTailRepairSyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{segment.PointBeforeTailTruncate, segment.PointBeforeTailSync}
	for _, point := range points {
		point := point
		for _, cause := range activeDataCreateCauses() {
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
				if err := batch.Put(context.Background(), id, []byte("active-tail")); err != nil {
					t.Fatal(err)
				}
				committed, err := batch.Commit(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				activeID := store.catalog.Snapshot().ActiveDataSegmentID
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "data", fmt.Sprintf("DATA-%08d.active", activeID))
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte("incomplete-tail")); err != nil {
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := openWithHook(cfg, errorAtActiveDataPoint(point, cause.err)); !errors.Is(err, cause.err) {
					t.Fatalf("faulted Open error=%v", err)
				}
				recovered, err := Open(cfg)
				if err != nil {
					t.Fatalf("retry Open: %v", err)
				}
				record, err := recovered.GetRecord(context.Background(), id)
				if err != nil || record.Revision != Revision(committed.BatchID) || string(record.Value) != "active-tail" {
					t.Fatalf("record=%+v error=%v", record, err)
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

func TestActiveDataCreateRecoveryRejectsSymlinkWithoutDeletingTarget(t *testing.T) {
	cfg := prepareRotationRecovery(t, rotation.PointOldSealed)
	current, err := initialize.Open(cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	newActive := filepath.Join(cfg.Dir, "data", fmt.Sprintf("DATA-%08d.active", current.NextDataSegmentID))
	if err := os.Symlink(target, newActive); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error=%v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "keep" {
		t.Fatalf("target=%q error=%v", data, err)
	}
}

type activeDataCause struct {
	name string
	err  error
}

func activeDataCreateCauses() []activeDataCause {
	return []activeDataCause{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
}

func errorAtActiveDataPoint(point failpoint.Point, cause error) failpoint.Hook {
	return failpoint.Func(func(got failpoint.Point) error {
		if got == point {
			return cause
		}
		return nil
	})
}

func prepareRotationRecovery(t *testing.T, point failpoint.Point) Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := createWithOptions(cfg, initialize.Options{Hook: errorAtActiveDataPoint(point, syscall.EBUSY)})
	if err != nil {
		t.Fatal(err)
	}
	var operationErr error
	for i := 0; i < 100 && operationErr == nil; i++ {
		batch, err := store.Begin(context.Background())
		if err != nil {
			operationErr = err
			break
		}
		id, err := batch.Allocate(context.Background())
		if err == nil {
			err = batch.Put(context.Background(), id, bytes.Repeat([]byte{'x'}, 512))
		}
		if err == nil {
			_, err = batch.Commit(context.Background())
		}
		operationErr = err
	}
	if !errors.Is(operationErr, syscall.EBUSY) {
		t.Fatalf("rotation preparation error=%v", operationErr)
	}
	_ = store.Close()
	return cfg
}

func assertRotationRecoveryWritable(t *testing.T, cfg Config) {
	t.Helper()
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("retry Open: %v", err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(context.Background())
	if err == nil {
		err = batch.Put(context.Background(), id, []byte("after-create-recovery"))
	}
	if err == nil {
		_, err = batch.Commit(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := verify.Run(context.Background(), cfg.Dir)
	if err != nil || !report.Clean {
		t.Fatalf("verify=%+v error=%v", report, err)
	}
}

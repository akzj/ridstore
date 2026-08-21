package ridstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/maintenance"
	"github.com/akzj/ridstore/internal/verify"
)

func TestDataGCTrashSyscallErrorsFailClosedAndRecover(t *testing.T) {
	for _, point := range dataGCTrashSyscallPoints() {
		point := point
		for _, cause := range dataGCSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				cfg := smallTestConfig(dir)
				cfg.SegmentSize = 16 << 10
				store, stableID, revision := prepareDataGCStore(t, cfg)
				store.hook = errorAtDataGCPoint(point, cause.err)
				if _, err := store.CompactData(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("CompactData error=%v", err)
				}
				assertMaintenanceFaultAndRecovery(t, store, cfg, stableID, revision, cause.err)
			})
		}
	}
}

func TestDataGCRecoveryTrashSyscallErrorsAreRetryable(t *testing.T) {
	cases := []struct {
		name    string
		prepare failpoint.Point
		points  []failpoint.Point
	}{
		{
			name:    "trash",
			prepare: pointDataGCManifestRemoved,
			points: []failpoint.Point{
				pointBeforeDataGCTrashRename,
				pointBeforeDataGCDataDirSync,
				pointBeforeDataGCTrashPublishDirSync,
			},
		},
		{
			name:    "delete",
			prepare: pointDataGCTrashed,
			points: []failpoint.Point{
				pointBeforeDataGCTrashDelete,
				pointBeforeDataGCTrashDeleteDirSync,
			},
		},
	}
	for _, test := range cases {
		test := test
		for _, point := range test.points {
			point := point
			for _, cause := range dataGCSyscallCauses() {
				cause := cause
				t.Run(test.name+"/"+string(point)+"/"+cause.name, func(t *testing.T) {
					dir := filepath.Join(t.TempDir(), "store")
					cfg := smallTestConfig(dir)
					cfg.SegmentSize = 16 << 10
					store, stableID, revision := prepareDataGCStore(t, cfg)
					store.hook = errorAtDataGCPoint(test.prepare, syscall.EBUSY)
					if _, err := store.CompactData(context.Background()); !errors.Is(err, syscall.EBUSY) {
						t.Fatalf("prepare CompactData error=%v", err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					if _, err := openWithHook(cfg, errorAtDataGCPoint(point, cause.err)); !errors.Is(err, cause.err) {
						t.Fatalf("recovery error=%v", err)
					}
					assertDataGCRecovered(t, cfg, stableID, revision)
				})
			}
		}
	}
}

func TestDataGCRecoveryPropagatesMaintenanceHook(t *testing.T) {
	for _, point := range []failpoint.Point{maintenance.PointBeforeWrite, maintenance.PointBeforeRemove} {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, stableID, revision := prepareDataGCStore(t, cfg)
			store.hook = errorAtDataGCPoint(pointDataGCTrashed, syscall.EBUSY)
			if _, err := store.CompactData(context.Background()); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("prepare CompactData error=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := openWithHook(cfg, errorAtDataGCPoint(point, syscall.EIO)); !errors.Is(err, syscall.EIO) {
				t.Fatalf("recovery error=%v", err)
			}
			assertDataGCRecovered(t, cfg, stableID, revision)
		})
	}
}

type dataGCCause struct {
	name string
	err  error
}

func dataGCSyscallCauses() []dataGCCause {
	return []dataGCCause{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
}

func dataGCTrashSyscallPoints() []failpoint.Point {
	return []failpoint.Point{
		pointBeforeDataGCTrashRename,
		pointBeforeDataGCDataDirSync,
		pointBeforeDataGCTrashPublishDirSync,
		pointBeforeDataGCTrashDelete,
		pointBeforeDataGCTrashDeleteDirSync,
	}
}

func errorAtDataGCPoint(point failpoint.Point, cause error) failpoint.Hook {
	return failpoint.Func(func(got failpoint.Point) error {
		if got == point {
			return cause
		}
		return nil
	})
}

func assertDataGCRecovered(t *testing.T, cfg Config, stableID ID, revision Revision) {
	t.Helper()
	recovered, err := Open(cfg)
	if err != nil {
		t.Fatalf("retry Open: %v", err)
	}
	assertMaintenanceGoneAndRecord(t, recovered, stableID, revision)
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(filepath.Join(cfg.Dir, "trash")); err != nil || len(entries) != 0 {
		t.Fatalf("trash entries=%v error=%v", entries, err)
	}
	report, err := verify.Run(context.Background(), cfg.Dir)
	if err != nil || !report.Clean {
		t.Fatalf("verify=%+v error=%v", report, err)
	}
}

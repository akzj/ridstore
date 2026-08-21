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

func TestMaintenanceJournalDirSyncErrorRespectsEveryDataGCPhase(t *testing.T) {
	for failCall := 1; failCall <= 7; failCall++ {
		failCall := failCall
		t.Run(string(rune('0'+failCall)), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, stableID, revision := prepareDataGCStore(t, cfg)
			calls := 0
			store.hook = failpoint.Func(func(point failpoint.Point) error {
				if point == maintenance.PointBeforeDirSync {
					calls++
					if calls == failCall {
						return syscall.EIO
					}
				}
				return nil
			})
			if _, err := store.CompactData(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("CompactData error=%v", err)
			}
			if calls != failCall {
				t.Fatalf("journal dir-sync calls=%d want=%d", calls, failCall)
			}
			if failCall <= 3 {
				if store.fault != nil {
					t.Fatalf("store faulted before checkpoint: %v", store.fault)
				}
				assertMaintenanceGoneAndRecord(t, store, stableID, revision)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			assertMaintenanceFaultAndRecovery(t, store, cfg, stableID, revision, syscall.EIO)
		})
	}
}

func TestMaintenanceJournalInstallErrorsAtCheckpointBoundaryRecover(t *testing.T) {
	points := []failpoint.Point{
		maintenance.PointBeforeTempRemove,
		maintenance.PointBeforeWrite,
		maintenance.PointBeforeFileSync,
		maintenance.PointBeforeRename,
		maintenance.PointBeforeDirSync,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, stableID, revision := prepareDataGCStore(t, cfg)
			calls := 0
			store.hook = failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					calls++
					if calls == 4 {
						return syscall.EIO
					}
				}
				return nil
			})
			if _, err := store.CompactData(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("CompactData error=%v", err)
			}
			if calls != 4 {
				t.Fatalf("journal calls=%d want=4", calls)
			}
			assertMaintenanceFaultAndRecovery(t, store, cfg, stableID, revision, syscall.EIO)
		})
	}
}

func TestMaintenanceJournalCleanupErrorsFailClosed(t *testing.T) {
	points := []failpoint.Point{
		maintenance.PointBeforeRemove,
		maintenance.PointBeforeRemoveTemp,
		maintenance.PointBeforeRemoveDirSync,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, stableID, revision := prepareDataGCStore(t, cfg)
			store.hook = failpoint.Func(func(got failpoint.Point) error {
				switch got {
				case pointDataGCCopying:
					return context.Canceled
				case point:
					return syscall.EIO
				default:
					return nil
				}
			})
			if _, err := store.CompactData(context.Background()); !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("CompactData error=%v", err)
			}
			assertMaintenanceFaultAndRecovery(t, store, cfg, stableID, revision, syscall.EIO)
		})
	}
}

func TestMaintenanceJournalFinalRemoveErrorsRecover(t *testing.T) {
	points := []failpoint.Point{
		maintenance.PointBeforeRemove,
		maintenance.PointBeforeRemoveTemp,
		maintenance.PointBeforeRemoveDirSync,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, stableID, revision := prepareDataGCStore(t, cfg)
			store.hook = failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return syscall.EIO
				}
				return nil
			})
			if _, err := store.CompactData(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("CompactData error=%v", err)
			}
			assertMaintenanceFaultAndRecovery(t, store, cfg, stableID, revision, syscall.EIO)
		})
	}
}

func assertMaintenanceGoneAndRecord(t *testing.T, store *Store, id ID, revision Revision) {
	t.Helper()
	if _, found, err := maintenance.Load(store.config.Dir); err != nil || found {
		t.Fatalf("journal found=%v error=%v", found, err)
	}
	record, err := store.GetRecord(context.Background(), id)
	if err != nil || record.Revision != revision || string(record.Value) != "stable" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}

func assertMaintenanceFaultAndRecovery(t *testing.T, store *Store, cfg Config, id ID, revision Revision, cause error) {
	t.Helper()
	if store.fault == nil || !errors.Is(store.fault, cause) {
		t.Fatalf("store fault=%v", store.fault)
	}
	if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, cause) {
		t.Fatalf("write after maintenance error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close faulted store: %v", err)
	}
	recovered, err := Open(cfg)
	if err != nil {
		t.Fatalf("fresh Open: %v", err)
	}
	assertMaintenanceGoneAndRecord(t, recovered, id, revision)
	if _, err := os.Lstat(filepath.Join(cfg.Dir, "journal", ".MAINTENANCE.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan maintenance temp remains: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := verify.Run(context.Background(), cfg.Dir)
	if err != nil || !report.Clean {
		t.Fatalf("verify=%+v error=%v", report, err)
	}
}

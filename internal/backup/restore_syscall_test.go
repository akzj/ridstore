package backup_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/verify"
)

func assertRestoreIncomplete(t *testing.T, destination string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(destination, initialize.RestoringMarkerFileName)); err != nil {
		t.Fatalf("RESTORING marker missing: %v", err)
	}
	if _, err := ridstore.Open(testConfig(destination)); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("incomplete restore opened: %v", err)
	}
	if _, err := verify.Run(context.Background(), destination); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("incomplete restore verified: %v", err)
	}
}

func TestRestoreArtifactSyscallErrorsRemainExplicitlyIncomplete(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	points := []failpoint.Point{
		backup.PointBeforeRestoreRootCreate,
		backup.PointBeforeRestoreMarkerWrite,
		backup.PointBeforeRestoreMarkerFileSync,
		backup.PointBeforeRestorePreparedRootSync,
		backup.PointBeforeRestoreParentSync,
		backup.PointBeforeRestorePayloadRootCreate,
		backup.PointBeforeRestorePayloadDirectoryCreate,
		backup.PointBeforeRestoreLockWrite,
		backup.PointBeforeRestoreLockFileSync,
		backup.PointBeforeRestorePayloadWrite,
		backup.PointBeforeRestorePayloadFileSync,
		backup.PointBeforeRestoreSegmentHeaderWrite,
		backup.PointBeforeRestoreSegmentHeaderFileSync,
		backup.PointBeforeRestoreManifestWrite,
		backup.PointBeforeRestoreManifestFileSync,
		backup.PointBeforeRestoreManifestRename,
		backup.PointBeforeRestoreManifestDirectorySync,
		backup.PointBeforeRestorePreparedManifestSync,
		backup.PointBeforeRestorePreparedDataSync,
		backup.PointBeforeRestorePreparedMapSync,
		backup.PointBeforeRestorePreparedJournalSync,
		backup.PointBeforeRestorePreparedTrashSync,
		backup.PointBeforeRestorePreparedTempSync,
		backup.PointBeforeRestorePreparedPayloadSync,
		backup.PointBeforeRestorePreparedLayoutSync,
		backup.PointBeforeRestorePayloadRename,
		backup.PointBeforeRestoreMovedPayloadSync,
		backup.PointBeforeRestorePayloadRootRemove,
		backup.PointBeforeRestorePublishedLayoutSync,
		backup.PointBeforeRestoreMarkerRemove,
		backup.PointBeforeRestorePublishRootSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range backupSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				destination := filepath.Join(t.TempDir(), "restored")
				report, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{
					Hook: backupOneShot(point, cause.err),
				})
				if !errors.Is(err, cause.err) {
					t.Fatalf("restore error=%v want cause=%v report=%+v", err, cause.err, report)
				}
				_, statErr := os.Lstat(destination)
				if point == backup.PointBeforeRestoreRootCreate {
					if !errors.Is(statErr, os.ErrNotExist) {
						t.Fatalf("destination created before root syscall: %v", statErr)
					}
					return
				}
				if statErr != nil {
					t.Fatalf("incomplete destination missing: %v", statErr)
				}
				assertRestoreIncomplete(t, destination)
			})
		}
	}

	if _, err := backup.Inspect(ctx, artifact); err != nil {
		t.Fatalf("source artifact changed by failed restores: %v", err)
	}
}

func TestRestorePartialPayloadPublicationRemainsFailClosed(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	for failCall := 2; failCall <= 8; failCall++ {
		failCall := failCall
		t.Run(strconv.Itoa(failCall), func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "restored")
			calls := 0
			hook := failpoint.Func(func(got failpoint.Point) error {
				if got == backup.PointBeforeRestorePayloadRename {
					calls++
					if calls == failCall {
						return syscall.EIO
					}
				}
				return nil
			})
			if _, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{Hook: hook}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("restore error=%v", err)
			}
			if calls != failCall {
				t.Fatalf("rename calls=%d want=%d", calls, failCall)
			}
			assertRestoreIncomplete(t, destination)
		})
	}
}

func TestRestorePartialUUIDRewriteRemainsFailClosed(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	destination := filepath.Join(t.TempDir(), "restored")
	calls := 0
	hook := failpoint.Func(func(got failpoint.Point) error {
		if got == backup.PointBeforeRestoreSegmentHeaderWrite {
			calls++
			if calls == 2 {
				return syscall.EIO
			}
		}
		return nil
	})
	if _, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{Hook: hook}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("restore error=%v", err)
	}
	if calls != 2 {
		t.Fatalf("header write calls=%d want=2", calls)
	}
	assertRestoreIncomplete(t, destination)
}

func TestRestoreManifestCleanupSyscallErrorsPreserveBothCauses(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	for _, cause := range backupSyscallCauses() {
		cause := cause
		t.Run(cause.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "restored")
			hook := failpoint.Func(func(got failpoint.Point) error {
				switch got {
				case backup.PointBeforeRestoreManifestWrite:
					return syscall.EBUSY
				case backup.PointBeforeRestoreManifestCleanupRemove:
					return cause.err
				default:
					return nil
				}
			})
			_, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{Hook: hook})
			if !errors.Is(err, syscall.EBUSY) || !errors.Is(err, cause.err) {
				t.Fatalf("restore error=%v want write and cleanup causes", err)
			}
			assertRestoreIncomplete(t, destination)
		})
	}
}

func TestRestorePublicationCompensationSyscallErrorsPreserveBothCauses(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	points := []failpoint.Point{
		backup.PointBeforeRestoreRecoveryMarkerWrite,
		backup.PointBeforeRestoreRecoveryMarkerFileSync,
		backup.PointBeforeRestoreRecoveryRootSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range backupSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				destination := filepath.Join(t.TempDir(), "restored")
				hook := failpoint.Func(func(got failpoint.Point) error {
					switch got {
					case backup.PointBeforeRestorePublishRootSync:
						return syscall.EBUSY
					case point:
						return cause.err
					default:
						return nil
					}
				})
				_, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{Hook: hook})
				if !errors.Is(err, syscall.EBUSY) || !errors.Is(err, cause.err) {
					t.Fatalf("restore error=%v want publication and compensation causes", err)
				}
				assertRestoreIncomplete(t, destination)
			})
		}
	}
}

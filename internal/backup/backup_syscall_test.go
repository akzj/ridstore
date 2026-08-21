package backup_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/verify"
)

type backupSyscallCause struct {
	name string
	err  error
}

func backupSyscallCauses() []backupSyscallCause {
	return []backupSyscallCause{
		{"EIO", syscall.EIO},
		{"ENOSPC", syscall.ENOSPC},
		{"EACCES", syscall.EACCES},
	}
}

func backupOneShot(point failpoint.Point, cause error) failpoint.Hook {
	fired := false
	return failpoint.Func(func(got failpoint.Point) error {
		if got == point && !fired {
			fired = true
			return cause
		}
		return nil
	})
}

func TestBackupArtifactSyscallErrorsRemainExplicitlyIncomplete(t *testing.T) {
	ctx := context.Background()
	source, _, _ := createStoreWithRecord(t, ctx)
	points := []failpoint.Point{
		backup.PointBeforeBackupRootCreate,
		backup.PointBeforeBackupMarkerWrite,
		backup.PointBeforeBackupMarkerFileSync,
		backup.PointBeforeBackupPreparedRootSync,
		backup.PointBeforeBackupParentSync,
		backup.PointBeforeBackupPayloadRootCreate,
		backup.PointBeforeBackupPayloadDirectoryCreate,
		backup.PointBeforeBackupPayloadWrite,
		backup.PointBeforeBackupPayloadFileSync,
		backup.PointBeforeBackupVerifyTrashCreate,
		backup.PointBeforeBackupVerifyLockWrite,
		backup.PointBeforeBackupVerifyLockFileSync,
		backup.PointBeforeBackupVerifyLockRemove,
		backup.PointBeforeBackupVerifyTrashRemove,
		backup.PointBeforeBackupMetadataWrite,
		backup.PointBeforeBackupMetadataFileSync,
		backup.PointBeforeBackupManifestDirectorySync,
		backup.PointBeforeBackupDataDirectorySync,
		backup.PointBeforeBackupMapDirectorySync,
		backup.PointBeforeBackupPayloadDirectorySync,
		backup.PointBeforeBackupMetadataRootSync,
		backup.PointBeforeBackupMarkerRemove,
		backup.PointBeforeBackupPublishRootSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range backupSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				destination := filepath.Join(t.TempDir(), "backup")
				report, err := backup.CreateWithOptions(ctx, source, destination, backup.CreateOptions{
					Hook: backupOneShot(point, cause.err),
				})
				if !errors.Is(err, cause.err) {
					t.Fatalf("create error=%v want cause=%v report=%+v", err, cause.err, report)
				}

				_, statErr := os.Lstat(destination)
				if point == backup.PointBeforeBackupRootCreate {
					if !errors.Is(statErr, os.ErrNotExist) {
						t.Fatalf("destination created before root syscall: %v", statErr)
					}
					return
				}
				if statErr != nil {
					t.Fatalf("incomplete destination missing: %v", statErr)
				}
				if _, err := backup.Inspect(ctx, destination); !errors.Is(err, base.ErrRecoveryRequired) {
					t.Fatalf("artifact became publishable after %s: %v", point, err)
				}
			})
		}
	}

	report, err := verify.Run(ctx, source)
	if err != nil || !report.Clean {
		t.Fatalf("source changed by failed backups: report=%+v error=%v", report, err)
	}
}

func TestBackupPublicationCompensationSyscallErrorsPreserveBothCauses(t *testing.T) {
	ctx := context.Background()
	source, _, _ := createStoreWithRecord(t, ctx)
	points := []failpoint.Point{
		backup.PointBeforeBackupRecoveryMarkerWrite,
		backup.PointBeforeBackupRecoveryMarkerFileSync,
		backup.PointBeforeBackupRecoveryRootSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range backupSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				destination := filepath.Join(t.TempDir(), "backup")
				hook := failpoint.Func(func(got failpoint.Point) error {
					switch got {
					case backup.PointBeforeBackupPublishRootSync:
						return syscall.EBUSY
					case point:
						return cause.err
					default:
						return nil
					}
				})
				_, err := backup.CreateWithOptions(ctx, source, destination, backup.CreateOptions{Hook: hook})
				if !errors.Is(err, syscall.EBUSY) || !errors.Is(err, cause.err) {
					t.Fatalf("create error=%v want publication and compensation causes", err)
				}
				if _, err := backup.Inspect(ctx, destination); !errors.Is(err, base.ErrRecoveryRequired) {
					t.Fatalf("artifact is not explicitly incomplete: %v", err)
				}
			})
		}
	}
}

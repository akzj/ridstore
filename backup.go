package ridstore

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/backuprestore"
	"github.com/akzj/ridstore/internal/verifier"
)

// BackupConfig describes an offline full backup. SourceDir must be a closed v2
// Store and DestDir must not exist. Verification limits receive Verify's
// defaults when zero.
type BackupConfig struct {
	SourceDir         string
	DestDir           string
	MappingCacheBytes uint64
	MaxLiveIDs        uint64
	MaxReplayStatuses uint64
}

// RestoreConfig describes an offline full restore. BackupDir is immutable for
// the duration of the call and DestDir must not exist. Verification limits
// receive Verify's defaults when zero.
type RestoreConfig struct {
	BackupDir         string
	DestDir           string
	MappingCacheBytes uint64
	MaxLiveIDs        uint64
	MaxReplayStatuses uint64
}

// BackupReport identifies the byte-exact Store image in an artifact.
// Restoring it preserves StoreID, so the original and any restored copies must
// never be used as simultaneous writers.
type BackupReport struct {
	StoreID            [16]byte
	ManifestGeneration uint64
	Files              uint64
	Bytes              uint64
}

// Backup creates and atomically publishes a v2-only offline full backup.
func Backup(ctx context.Context, config BackupConfig) (BackupReport, error) {
	report, err := backuprestore.Backup(ctx, backuprestore.Config{
		SourceDir: config.SourceDir, DestDir: config.DestDir,
		Verify: backupVerifyConfig(config.MappingCacheBytes, config.MaxLiveIDs, config.MaxReplayStatuses),
	})
	if errors.Is(err, verifier.ErrLimit) {
		err = errors.Join(ErrVerifyLimit, err)
	}
	return publicBackupReport(report), err
}

// Restore verifies and atomically publishes a v2 Store into a new directory.
// It preserves StoreID and is disaster recovery, not cloning: the original and
// restored Store must not be opened as simultaneous writers.
func Restore(ctx context.Context, config RestoreConfig) (BackupReport, error) {
	report, err := backuprestore.Restore(ctx, backuprestore.Config{
		SourceDir: config.BackupDir, DestDir: config.DestDir,
		Verify: backupVerifyConfig(config.MappingCacheBytes, config.MaxLiveIDs, config.MaxReplayStatuses),
	})
	if errors.Is(err, verifier.ErrLimit) {
		err = errors.Join(ErrVerifyLimit, err)
	}
	return publicBackupReport(report), err
}

func backupVerifyConfig(mappingCacheBytes, maxLiveIDs, maxReplayStatuses uint64) verifier.Config {
	defaults := (VerifyConfig{
		MappingCacheBytes: mappingCacheBytes,
		MaxLiveIDs:        maxLiveIDs, MaxReplayStatuses: maxReplayStatuses,
	}).withDefaults()
	return verifier.Config{
		MappingCacheBytes: defaults.MappingCacheBytes,
		MaxLiveIDs:        defaults.MaxLiveIDs,
		MaxReplayStatuses: defaults.MaxReplayStatuses,
	}
}

func publicBackupReport(report backuprestore.Report) BackupReport {
	return BackupReport{
		StoreID: report.StoreID, ManifestGeneration: report.ManifestGeneration,
		Files: report.Files, Bytes: report.Bytes,
	}
}

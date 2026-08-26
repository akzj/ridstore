package verifier

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/bootstrap"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

type Stage string

var ErrLimit = errors.New("verifier: live ID limit exceeded")

const (
	StageLocked    Stage = "locked"
	StageManifest  Stage = "manifest"
	StageRecordLog Stage = "recordlog"
	StageMapping   Stage = "mapping"
	StagePhysical  Stage = "physical-complete"
	StageReachable Stage = "mapping-reachable"
)

type Config struct {
	MappingCacheBytes uint64
	MaxLiveIDs        uint64
}

type Report struct {
	Stage              Stage
	ManifestGeneration uint64
	Data               recordlog.PhysicalReport
	Mapping            mapstore.PhysicalReport
	LiveIDs            uint64
}

// Verify validates stable v2 physical files and the reachable checkpoint
// Mapping under an exclusive read-only lease. It never invokes recovery or a
// writer path.
func Verify(ctx context.Context, root string, config Config) (report Report, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if root == "" || config.MappingCacheBytes == 0 || config.MaxLiveIDs == 0 {
		return report, base.ErrInvalidConfig
	}
	lock, err := filelock.AcquireExisting(root)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	report.Stage = StageLocked

	if found, err := bootstrap.RecoveryArtifacts(root); err != nil {
		return report, err
	} else if found {
		return report, base.ErrRecoveryRequired
	}
	if found, err := maintstate.RecoveryArtifacts(root); err != nil {
		return report, err
	} else if found {
		return report, base.ErrRecoveryRequired
	}
	manifest, err := storecatalog.LoadStrict(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return report, errors.Join(base.ErrNotInitialized, err)
		}
		return report, classify(err)
	}
	report.Stage = StageManifest
	report.ManifestGeneration = manifest.Generation

	report.Data, err = recordlog.VerifyFiles(ctx, root, manifest.RecordLogSnapshot())
	if err != nil {
		return report, classify(err)
	}
	report.Stage = StageRecordLog
	reader, mappingReport, err := mapstore.OpenVerifiedReader(ctx, root, manifest.MapStoreSnapshot())
	if err != nil {
		return report, classify(err)
	}
	defer func() { resultErr = errors.Join(resultErr, reader.Close()) }()
	report.Mapping = mappingReport
	report.Stage = StageMapping
	if err := verifyJournalAndTrash(root); err != nil {
		return report, err
	}
	report.Stage = StagePhysical
	tree, err := radix.OpenReadOnly(reader, manifest.MappingRoot, manifest.CoveredCommitSeq, config.MappingCacheBytes)
	if err != nil {
		return report, classify(err)
	}
	addresses := make(map[recordlog.VAddr]model.ID)
	var previous model.ID
	err = tree.Walk(ctx, func(id model.ID, addr recordlog.VAddr) error {
		if report.LiveIDs == config.MaxLiveIDs {
			return ErrLimit
		}
		if id <= previous || !containsDataAddress(manifest, report.Data, addr) {
			return base.ErrCorrupt
		}
		if owner, exists := addresses[addr]; exists {
			return errors.Join(base.ErrCorrupt, fmt.Errorf("data address %v aliases IDs %d and %d", addr, owner, id))
		}
		addresses[addr] = id
		previous = id
		report.LiveIDs++
		return nil
	})
	if err != nil {
		return report, classify(err)
	}
	report.Stage = StageReachable
	return report, nil
}

func containsDataAddress(manifest storecatalog.Manifest, data recordlog.PhysicalReport, addr recordlog.VAddr) bool {
	if !addr.Valid() || addr.Offset() < recordlog.SegmentHeaderSize {
		return false
	}
	if addr.SegmentID() == manifest.ActiveDataSegmentID {
		return addr.Offset() < data.ActiveEnd.Offset
	}
	for _, summary := range manifest.SealedDataSegments {
		if addr.SegmentID() == summary.SegmentID {
			return addr.Offset() < summary.ValidEnd
		}
	}
	return false
}

func verifyJournalAndTrash(root string) error {
	journal := filepath.Join(root, "journal")
	if err := requireEmptyDirectory(journal, false); err != nil {
		return err
	}
	trash := filepath.Join(root, "trash")
	if err := requireEmptyDirectory(trash, true); err != nil {
		return err
	}
	return nil
}

func requireEmptyDirectory(path string, recoveryIfNonEmpty bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(base.ErrCorrupt, fmt.Errorf("%s is not a directory", path))
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if recoveryIfNonEmpty {
		return base.ErrRecoveryRequired
	}
	return errors.Join(base.ErrCorrupt, fmt.Errorf("unexpected journal file %q", entries[0].Name()))
}

func classify(err error) error {
	switch {
	case errors.Is(err, storecatalog.ErrRecoveryRequired), errors.Is(err, recordlog.ErrRecoveryRequired), errors.Is(err, mapstore.ErrRecoveryRequired):
		return errors.Join(base.ErrRecoveryRequired, err)
	case errors.Is(err, storecatalog.ErrUnsupported), errors.Is(err, recordlog.ErrUnsupported), errors.Is(err, mapstore.ErrUnsupported):
		return errors.Join(base.ErrUnsupported, err)
	case errors.Is(err, storecatalog.ErrCorrupt), errors.Is(err, storecatalog.ErrInvalid), errors.Is(err, recordlog.ErrCorrupt), errors.Is(err, mapstore.ErrCorrupt):
		return errors.Join(base.ErrCorrupt, err)
	case errors.Is(err, radix.ErrCorrupt), errors.Is(err, radix.ErrInvalid):
		return errors.Join(base.ErrCorrupt, err)
	case errors.Is(err, os.ErrNotExist):
		return errors.Join(base.ErrCorrupt, err)
	default:
		return err
	}
}

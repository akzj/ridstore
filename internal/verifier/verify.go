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
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/replay"
	"github.com/akzj/ridstore/internal/segmentstats"
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
	StageSemantic  Stage = "semantic-replay"
	StageExact     Stage = "exact-join"
)

type Config struct {
	MappingCacheBytes uint64
	MaxLiveIDs        uint64
	MaxReplayStatuses uint64
}

type Report struct {
	Stage              Stage
	ManifestGeneration uint64
	StoreID            [16]byte
	Data               recordlog.PhysicalReport
	Mapping            mapstore.PhysicalReport
	CheckpointLiveIDs  uint64
	LiveIDs            uint64
	ReplayedCommits    uint64
	BatchStatuses      uint64
	NextCommitSeq      model.CommitSeq
	VerifiedPuts       uint64
	VerifiedStats      uint64
}

// Verify validates stable v2 physical files, the checkpoint Mapping, semantic
// replay, and the final Mapping-to-Record join under an exclusive read-only
// lease. It never invokes recovery or a writer path.
func Verify(ctx context.Context, root string, config Config) (report Report, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if root == "" || config.MappingCacheBytes == 0 || config.MaxLiveIDs == 0 || config.MaxReplayStatuses == 0 {
		return report, base.ErrInvalidConfig
	}
	lock, err := filelock.AcquireExisting(root)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	return VerifyHeld(ctx, root, config)
}

// VerifyHeld is Verify's validation core for callers that already own the
// store directory's exclusive lease. Keeping verification and a following
// file-set operation under one lease prevents the validated Manifest and
// active tails from changing between those operations.
func VerifyHeld(ctx context.Context, root string, config Config) (report Report, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if root == "" || config.MappingCacheBytes == 0 || config.MaxLiveIDs == 0 || config.MaxReplayStatuses == 0 {
		return report, base.ErrInvalidConfig
	}
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
	if found, err := mapgcstate.RecoveryArtifacts(root); err != nil {
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
	report.StoreID = manifest.StoreUUID

	dataReader, dataReport, err := recordlog.OpenVerifiedReader(ctx, root, manifest.RecordLogSnapshot())
	if err != nil {
		return report, classify(err)
	}
	defer func() { resultErr = errors.Join(resultErr, dataReader.Close()) }()
	report.Data = dataReport
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
	entries := make(map[model.ID]recordlog.VAddr)
	var previous model.ID
	err = tree.Walk(ctx, func(id model.ID, addr recordlog.VAddr) error {
		if report.CheckpointLiveIDs == config.MaxLiveIDs {
			return ErrLimit
		}
		if id <= previous || !containsDataAddress(manifest, report.Data, addr) {
			return base.ErrCorrupt
		}
		if owner, exists := addresses[addr]; exists {
			return errors.Join(base.ErrCorrupt, fmt.Errorf("data address %v aliases IDs %d and %d", addr, owner, id))
		}
		addresses[addr] = id
		entries[id] = addr
		previous = id
		report.CheckpointLiveIDs++
		return nil
	})
	if err != nil {
		return report, classify(err)
	}
	if report.CheckpointLiveIDs != manifest.MappingEntryCount {
		return report, base.ErrCorrupt
	}
	report.Stage = StageReachable
	baseMapping, err := mapping.New(mapping.Snapshot{CoveredCommitSeq: manifest.CoveredCommitSeq, Entries: entries})
	if err != nil {
		return report, classify(err)
	}
	bounded := &boundedMapping{Mapping: baseMapping, live: report.CheckpointLiveIDs, limit: config.MaxLiveIDs}
	recovered, err := replay.Recover(ctx, dataReader, replay.Checkpoint{
		Mapping: bounded, ReplayStart: manifest.ReplayStart,
		ReservedIDHigh: manifest.ReservedIDHigh, ReservedBatchIDHigh: manifest.ReservedBatchIDHigh,
		OpenBatchIDs: manifest.OpenBatchIDsAtCut,
	}, replay.Config{
		MaxValueSize: manifest.HardLimits.MaxValueSize, MaxRecordPayload: manifest.HardLimits.MaxRecordLogPayload,
		MaxGroupDescriptors: manifest.HardLimits.MaxRecordLogPayload / uint64(recordcodec.DescriptorHeadSize),
		MaxGroupMutations:   manifest.HardLimits.MaxRecordLogPayload / uint64(recordcodec.MutationSize),
		IDReserveSize:       manifest.HardLimits.IDReserveSize, BatchIDReserveSize: manifest.HardLimits.BatchIDReserveSize,
		StatusCapacity: config.MaxReplayStatuses,
	})
	if err != nil {
		return report, classify(err)
	}
	final := baseMapping.Snapshot()
	if uint64(len(final.Entries)) > config.MaxLiveIDs {
		return report, ErrLimit
	}
	report.LiveIDs = uint64(len(final.Entries))
	report.NextCommitSeq = recovered.NextCommitSeq
	report.ReplayedCommits = uint64(recovered.NextCommitSeq) - uint64(manifest.CoveredCommitSeq) - 1
	report.BatchStatuses = uint64(len(recovered.Statuses))
	report.Stage = StageSemantic
	finalAddresses := make(map[recordlog.VAddr]model.ID, len(final.Entries))
	for id, addr := range final.Entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if !containsDataAddress(manifest, report.Data, addr) {
			return report, base.ErrCorrupt
		}
		if owner, exists := finalAddresses[addr]; exists {
			return report, errors.Join(base.ErrCorrupt, fmt.Errorf("data address %v aliases IDs %d and %d", addr, owner, id))
		}
		payload, err := dataReader.Read(ctx, addr)
		if err != nil {
			return report, classify(err)
		}
		put, err := recordcodec.DecodePut(payload, manifest.HardLimits.MaxValueSize)
		if err != nil || put.RecordID != id {
			return report, errors.Join(base.ErrCorrupt, err)
		}
		finalAddresses[addr] = id
		report.VerifiedPuts++
	}
	maxStats := uint64(len(manifest.SealedDataSegments))
	if maxStats == 0 {
		maxStats = 1
	}
	stats, err := segmentstats.Build(ctx, tree, dataReader, nil, segmentstats.FileSet{
		Active: manifest.ActiveDataSegmentID, Sealed: manifest.SealedDataSegments,
	}, manifest.HardLimits.MaxValueSize, maxStats)
	if err != nil {
		return report, classify(err)
	}
	if !equalCoveredSegmentStats(stats, manifest.SegmentStats, manifest) {
		return report, base.ErrCorrupt
	}
	report.VerifiedStats = uint64(len(stats))
	report.Stage = StageExact
	return report, nil
}

// boundedMapping keeps verifier-owned replay state within the explicit live-ID
// budget. Replay is serial, so this wrapper can preflight each resolved group
// without adding synchronization beyond Mapping itself.
type boundedMapping struct {
	*mapping.Mapping
	live  uint64
	limit uint64
}

func (m *boundedMapping) PublishGroup(first model.CommitSeq, plan mapping.GroupPlan, reservations []mapping.DeltaReservation) (mapping.PublishResult, error) {
	type presence struct {
		exists bool
	}
	virtual := make(map[model.ID]presence)
	next := m.live
	for _, proposal := range plan.Proposals {
		if !proposal.Accepted {
			continue
		}
		for _, change := range proposal.Changes {
			if !change.Apply {
				continue
			}
			state, found := virtual[change.Change.RecordID]
			if !found {
				var err error
				_, state.exists, err = m.Mapping.Lookup(change.Change.RecordID)
				if err != nil {
					return mapping.PublishResult{}, err
				}
			}
			willExist := change.Change.Operation != mapping.OperationDelete
			if !state.exists && willExist {
				if next == m.limit {
					return mapping.PublishResult{}, ErrLimit
				}
				next++
			} else if state.exists && !willExist {
				next--
			}
			virtual[change.Change.RecordID] = presence{exists: willExist}
		}
	}
	result, err := m.Mapping.PublishGroup(first, plan, reservations)
	if err == nil {
		m.live = next
	}
	return result, err
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

func equalCoveredSegmentStats(exact, recorded []storecatalog.SegmentStats, manifest storecatalog.Manifest) bool {
	exactByID := make(map[recordlog.SegmentID]storecatalog.SegmentStats, len(exact))
	for _, stat := range exact {
		exactByID[stat.SegmentID] = stat
	}
	recordedByID := make(map[recordlog.SegmentID]storecatalog.SegmentStats, len(recorded))
	for _, stat := range recorded {
		want, ok := exactByID[stat.SegmentID]
		if ok && want != stat {
			return false
		}
		if !ok && (stat.LiveBytes != 0 || stat.LiveRecords != 0) {
			return false
		}
		recordedByID[stat.SegmentID] = stat
	}
	for _, segment := range manifest.SealedDataSegments {
		if !storecatalog.StatsKnownForSegment(manifest.ReplayStart, segment) {
			continue
		}
		if want, live := exactByID[segment.SegmentID]; live {
			if got, ok := recordedByID[segment.SegmentID]; !ok || got != want {
				return false
			}
		}
	}
	return true
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
	case errors.Is(err, ErrLimit):
		return ErrLimit
	case errors.Is(err, storecatalog.ErrRecoveryRequired), errors.Is(err, recordlog.ErrRecoveryRequired), errors.Is(err, mapstore.ErrRecoveryRequired):
		return errors.Join(base.ErrRecoveryRequired, err)
	case errors.Is(err, storecatalog.ErrUnsupported), errors.Is(err, recordlog.ErrUnsupported), errors.Is(err, mapstore.ErrUnsupported):
		return errors.Join(base.ErrUnsupported, err)
	case errors.Is(err, storecatalog.ErrCorrupt), errors.Is(err, storecatalog.ErrInvalid), errors.Is(err, recordlog.ErrCorrupt), errors.Is(err, mapstore.ErrCorrupt):
		return errors.Join(base.ErrCorrupt, err)
	case errors.Is(err, radix.ErrCorrupt), errors.Is(err, radix.ErrInvalid):
		return errors.Join(base.ErrCorrupt, err)
	case errors.Is(err, mapping.ErrCorrupt), errors.Is(err, mapping.ErrInvalid), errors.Is(err, mapping.ErrStalePlan):
		return errors.Join(base.ErrCorrupt, err)
	case errors.Is(err, os.ErrNotExist):
		return errors.Join(base.ErrCorrupt, err)
	default:
		return err
	}
}

package ridstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/maintenance"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/segment"
)

type DataGCResult struct {
	SourceSegmentID base.DataSegmentID
	SourceBytes     uint64
	CopiedBytes     uint64
	Relocated       uint64
	Skipped         uint64
}

const (
	pointDataGCPrepared        failpoint.Point = "data-gc.prepared"
	pointDataGCCopying         failpoint.Point = "data-gc.copying"
	pointDataGCRelocations     failpoint.Point = "data-gc.relocations-durable"
	pointDataGCCheckpoint      failpoint.Point = "data-gc.checkpoint-durable"
	pointDataGCRetired         failpoint.Point = "data-gc.retired"
	pointDataGCManifestRemoved failpoint.Point = "data-gc.manifest-removed"
	pointDataGCTrashed         failpoint.Point = "data-gc.trashed"
	pointDataGCDeleted         failpoint.Point = "data-gc.deleted"
)

type dataGCSession struct {
	store       *Store
	source      storeformat.FileSummary
	relocated   uint64
	skipped     uint64
	copiedBytes uint64
	lastCommit  base.CommitSeq
	cleaning    bool
	startedAt   time.Time
	throttled   time.Duration
}

func (s *Store) beginDataGC() (*dataGCSession, error) {
	manifest := s.catalog.Snapshot()
	candidates := dataGCCandidates(manifest)
	for _, candidate := range candidates {
		id := base.DataSegmentID(candidate.FileID)
		if s.segments.OpenBatchRefs(id) != 0 {
			continue
		}
		if err := s.segments.BeginCleaning(id); err != nil {
			if errors.Is(err, segment.ErrCleaning) || errors.Is(err, segment.ErrRetired) || errors.Is(err, base.ErrInvalidConfig) {
				continue
			}
			return nil, err
		}
		return &dataGCSession{store: s, source: candidate, cleaning: true, startedAt: time.Now()}, nil
	}
	return nil, base.ErrNotFound
}

func (s *Store) admitDataGCCopy(session *dataGCSession) error {
	if session == nil {
		return base.ErrInvalidConfig
	}
	availableFn := s.availableBytes
	if availableFn == nil {
		availableFn = defaultAvailableBytes
	}
	available, err := availableFn(s.config.Dir)
	if err != nil {
		return err
	}
	required, err := dataGCCopySpaceUpper(s.catalog.Snapshot(), session.source, s.config)
	if err != nil {
		return err
	}
	if available < required {
		return fmt.Errorf("data GC requires %d temporary bytes with %d available: %w", required, available, base.ErrInsufficientSpace)
	}
	return nil
}

func dataGCCopySpaceUpper(manifest storeformat.Manifest, source storeformat.FileSummary, cfg Config) (uint64, error) {
	var liveBytes, liveRecords uint64
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == base.DataSegmentID(source.FileID) {
			liveBytes, liveRecords = stat.ExactLiveBytes, stat.ExactLiveRecords
			break
		}
	}
	descriptorBytes, err := base.MulUint64(liveRecords, storeformat.MutationEntrySize)
	if err != nil {
		return 0, err
	}
	// Reserve two complete Segments for append/Mapping rotation boundaries and
	// Manifest/journal progress. Free-space observation is not a reservation,
	// so every write/fsync must still propagate ENOSPC.
	rotationBytes, err := base.MulUint64(uint64(cfg.SegmentSize), 2)
	if err != nil {
		return 0, err
	}
	required := uint64(cfg.GCMinFreeBytes)
	for _, value := range []uint64{liveBytes, descriptorBytes, rotationBytes} {
		required, err = base.AddUint64(required, value)
		if err != nil {
			return 0, err
		}
	}
	return required, nil
}

func (s *Store) admitDataGCCheckpoint(checkpoint *radix.Checkpoint) error {
	if checkpoint == nil {
		return base.ErrInvalidConfig
	}
	availableFn := s.availableBytes
	if availableFn == nil {
		availableFn = defaultAvailableBytes
	}
	available, err := availableFn(s.config.Dir)
	if err != nil {
		return err
	}
	// The barrier has frozen the exact layers this checkpoint will build. One
	// entry can rewrite at most one Dense512 node at each of eight radix levels.
	// Convert the node count to whole Segment bytes so headers, footers and tail
	// fragmentation are included in the conservative bound.
	maxNodeBytes := uint64(storeformat.MappingNodeHeaderSize + storeformat.MappingNodeSlots*8)
	usable := uint64(s.config.SegmentSize) - storeformat.SegmentHeaderSize - storeformat.SegmentFooterSize
	nodesPerSegment := usable / maxNodeBytes
	if nodesPerSegment == 0 {
		return base.ErrInvalidConfig
	}
	nodeCount, err := base.MulUint64(checkpoint.EntryCount(), 8)
	if err != nil {
		return err
	}
	segmentCount := nodeCount / nodesPerSegment
	if nodeCount%nodesPerSegment != 0 {
		segmentCount++
	}
	required, err := base.MulUint64(segmentCount, uint64(s.config.SegmentSize))
	if err != nil {
		return err
	}
	required, err = base.AddUint64(required, uint64(s.config.GCMinFreeBytes))
	if err != nil {
		return err
	}
	required, err = base.AddUint64(required, uint64(s.config.SegmentSize))
	if err != nil {
		return err
	}
	if available < required {
		return fmt.Errorf("data GC checkpoint requires %d temporary bytes for %d entries with %d available: %w", required, checkpoint.EntryCount(), available, base.ErrInsufficientSpace)
	}
	return nil
}

func dataGCCandidates(manifest storeformat.Manifest) []storeformat.FileSummary {
	live := make(map[base.DataSegmentID]uint64, len(manifest.SegmentStats))
	for _, stat := range manifest.SegmentStats {
		live[stat.SegmentID] = stat.ExactLiveBytes
	}
	replaySegment := manifest.ReplayStart.SegmentID()
	type scored struct {
		summary     storeformat.FileSummary
		reclaimable uint64
	}
	scoredCandidates := make([]scored, 0, len(manifest.SealedDataSegments))
	for _, summary := range manifest.SealedDataSegments {
		id := base.DataSegmentID(summary.FileID)
		if id >= replaySegment || summary.ValidEnd <= storeformat.SegmentHeaderSize {
			continue
		}
		physical := summary.ValidEnd - storeformat.SegmentHeaderSize
		reclaimable := uint64(0)
		if live[id] < physical {
			reclaimable = physical - live[id]
		}
		if reclaimable == 0 {
			continue
		}
		scoredCandidates = append(scoredCandidates, scored{summary: summary, reclaimable: reclaimable})
	}
	sort.Slice(scoredCandidates, func(i, j int) bool {
		if scoredCandidates[i].reclaimable != scoredCandidates[j].reclaimable {
			return scoredCandidates[i].reclaimable > scoredCandidates[j].reclaimable
		}
		return scoredCandidates[i].summary.FileID < scoredCandidates[j].summary.FileID
	})
	result := make([]storeformat.FileSummary, len(scoredCandidates))
	for i := range scoredCandidates {
		result[i] = scoredCandidates[i].summary
	}
	return result
}

func (g *dataGCSession) relocate(ctx context.Context) error {
	if g == nil || g.store == nil || !g.cleaning {
		return base.ErrInvalidConfig
	}
	s := g.store
	sourceID := base.DataSegmentID(g.source.FileID)
	changes := make([]api.Change, 0, s.config.GCBatchMutations)
	var batchBytes uint64
	flush := func() error {
		if len(changes) == 0 {
			return nil
		}
		sort.Slice(changes, func(i, j int) bool { return changes[i].RecordID < changes[j].RecordID })
		if err := s.reserveInternalStatusSlot(); err != nil {
			if errors.Is(err, base.ErrStatusCapacity) {
				s.requestCheckpoint()
			}
			return err
		}
		rawBatchID, err := s.batchAllocator.Allocate(ctx)
		if err != nil {
			return errors.Join(err, s.releaseInternalStatusSlot(nil))
		}
		batchID := base.BatchID(rawBatchID)
		s.mu.Lock()
		if rawBatchID >= s.issuedBatchHigh {
			s.issuedBatchHigh = rawBatchID + 1
		}
		s.mu.Unlock()
		result, err := s.coordinator.Relocate(ctx, batchID, changes)
		if err != nil {
			var status *BatchStatus
			if errors.Is(err, base.ErrCommitUnknown) {
				unknown := BatchStatus{BatchID: batchID, State: BatchStateCommitUnknown}
				status = &unknown
			}
			return errors.Join(err, s.releaseInternalStatusSlot(status))
		}
		committed := BatchStatus{BatchID: batchID, State: BatchStateCommitted, CommitSeq: result.CommitSeq}
		if err := s.releaseInternalStatusSlot(&committed); err != nil {
			s.setFault(err)
			return err
		}
		g.relocated += uint64(result.Applied)
		g.skipped += uint64(result.Skipped)
		g.lastCommit = result.CommitSeq
		changes = changes[:0]
		batchBytes = 0
		delay := gcThrottleDelay(g.copiedBytes, uint64(s.config.GCBytesPerSecond), time.Since(g.startedAt))
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			g.throttled += delay
			return nil
		}
	}
	err := s.segments.ScanCleaning(sourceID, func(oldAddr base.VAddr, frame storeformat.Frame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if frame.Type != storeformat.FrameTypePutRecord {
			return nil
		}
		current, exists, err := s.mapping.Lookup(frame.RecordID)
		if err != nil {
			return err
		}
		if !exists || current != oldAddr {
			return nil
		}
		valueBytes := uint64(len(frame.Payload))
		batchLimit := uint64(s.config.GCBatchBytes)
		if len(changes) != 0 && (len(changes) >= s.config.GCBatchMutations || batchBytes >= batchLimit || valueBytes > batchLimit-batchBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		newAddr, _, written, err := s.log.AppendPut(ctx, frame.BatchID, frame.RecordID, frame.Payload)
		if err != nil {
			return err
		}
		g.copiedBytes, err = base.AddUint64(g.copiedBytes, written)
		if err != nil {
			return err
		}
		batchBytes, err = base.AddUint64(batchBytes, valueBytes)
		if err != nil {
			return err
		}
		changes = append(changes, api.Change{RecordID: frame.RecordID, ExpectedOldAddr: oldAddr, NewAddr: newAddr})
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		if s.log.Faulted() || errors.Is(err, base.ErrCorrupt) || errors.Is(err, base.ErrCommitUnknown) {
			s.setFault(err)
		}
		return err
	}
	// SegmentStats selected the candidate; this second exact scan is the first
	// deletion proof. The later GC-required Checkpoint validates every Mapping
	// target again before the source can leave the Manifest.
	return g.validateSourceEmpty(ctx)
}

func gcThrottleDelay(copiedBytes, bytesPerSecond uint64, elapsed time.Duration) time.Duration {
	if copiedBytes == 0 || bytesPerSecond == 0 {
		return 0
	}
	seconds := copiedBytes / bytesPerSecond
	remainder := copiedBytes % bytesPerSecond
	if seconds > uint64(math.MaxInt64/int64(time.Second)) {
		return time.Duration(math.MaxInt64)
	}
	target := time.Duration(seconds) * time.Second
	if remainder != 0 {
		hi, lo := bits.Mul64(remainder, uint64(time.Second))
		nanos, _ := bits.Div64(hi, lo, bytesPerSecond)
		if nanos > uint64(math.MaxInt64-int64(target)) {
			return time.Duration(math.MaxInt64)
		}
		target += time.Duration(nanos)
	}
	if elapsed >= target {
		return 0
	}
	return target - elapsed
}

func (g *dataGCSession) validateSourceEmpty(ctx context.Context) error {
	s := g.store
	sourceID := base.DataSegmentID(g.source.FileID)
	return s.segments.ScanCleaning(sourceID, func(addr base.VAddr, frame storeformat.Frame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if frame.Type != storeformat.FrameTypePutRecord {
			return nil
		}
		current, exists, err := s.mapping.Lookup(frame.RecordID)
		if err != nil {
			return err
		}
		if exists && current == addr {
			return fmt.Errorf("source segment still contains live mapping for ID %d: %w", frame.RecordID, base.ErrConflict)
		}
		return nil
	})
}

func (g *dataGCSession) cancel() error {
	if g == nil || g.store == nil || !g.cleaning {
		return nil
	}
	err := g.store.segments.CancelCleaning(base.DataSegmentID(g.source.FileID))
	if err == nil {
		g.cleaning = false
	}
	return err
}

// CompactData cleans at most one checkpoint-safe sealed Data Segment. A zero
// result with nil error means no sealed Segment currently has reclaimable
// space below the durable replay boundary.
func (s *Store) CompactData(ctx context.Context) (result DataGCResult, resultErr error) {
	if s == nil {
		return DataGCResult{}, base.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return DataGCResult{}, err
	}
	// Establish exact SegmentStats and a replay boundary before choosing a
	// candidate. The operation then owns checkpointMu through final deletion.
	if err := s.Checkpoint(ctx); err != nil {
		return DataGCResult{}, err
	}
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.ops.RLock()
	defer s.ops.RUnlock()
	if err := s.checkAvailable(); err != nil {
		return DataGCResult{}, err
	}
	session, err := s.beginDataGC()
	if errors.Is(err, base.ErrNotFound) {
		s.metrics.GCNoCandidate()
		return DataGCResult{}, nil
	}
	if err != nil {
		return DataGCResult{}, err
	}
	s.metrics.GCStarted()
	startedAt := time.Now()
	defer func() {
		s.metrics.AddGCDuration(uint64(time.Since(startedAt)))
		s.metrics.AddGCThrottled(uint64(session.throttled))
		if resultErr != nil {
			s.metrics.GCFailed()
			return
		}
		s.metrics.GCCompleted()
		s.metrics.AddGCCopiedBytes(result.CopiedBytes)
		if result.SourceBytes >= result.CopiedBytes {
			s.metrics.AddGCReclaimedBytes(result.SourceBytes - result.CopiedBytes)
		}
		s.metrics.AddGCRelocated(result.Relocated)
		s.metrics.AddGCSkipped(result.Skipped)
	}()
	journalInstalled := false
	checkpointDurable := false
	defer func() {
		if resultErr == nil {
			return
		}
		if !checkpointDurable {
			cancelErr := session.cancel()
			var removeErr error
			if journalInstalled {
				removeErr = maintenance.RemoveWithHook(s.config.Dir, s.hook)
			}
			if cleanupErr := errors.Join(cancelErr, removeErr); cleanupErr != nil {
				resultErr = errors.Join(resultErr, cleanupErr)
				s.setFault(resultErr)
			}
			return
		}
		// Once the checkpoint proving no source mappings is durable, recovery
		// must finish the journal rather than silently roll the operation back.
		s.setFault(resultErr)
	}()
	if err := s.admitDataGCCopy(session); err != nil {
		if errors.Is(err, base.ErrInsufficientSpace) {
			s.metrics.GCInsufficientSpace()
		}
		return DataGCResult{}, err
	}
	current := s.catalog.Snapshot()
	if current.MaintenanceGeneration == ^uint64(0) {
		return DataGCResult{}, base.ErrGenerationExhausted
	}
	var operationID [16]byte
	if _, err := rand.Read(operationID[:]); err != nil {
		return DataGCResult{}, err
	}
	journal := storeformat.MaintenanceJournal{
		Generation: current.MaintenanceGeneration + 1, StoreUUID: current.StoreUUID, OperationID: operationID,
		OperationType: storeformat.MaintenanceDataGC, Phase: 1, OldManifestGeneration: current.Generation,
		SourceFiles: []storeformat.JournalFileRef{{
			Kind: storeformat.FileKindData, State: storeformat.FileStateSealed, FileID: session.source.FileID,
			ValidEnd: session.source.ValidEnd, FirstSeq: session.source.FirstSeq, LastSeq: session.source.LastSeq,
		}},
	}
	if _, found, err := maintenance.Load(s.config.Dir); err != nil {
		return DataGCResult{}, err
	} else if found {
		return DataGCResult{}, fmt.Errorf("maintenance journal already active: %w", base.ErrConflict)
	}
	// Even a failed install can leave a temp file or a renamed journal whose
	// directory sync outcome is unknown. After proving no prior owner exists,
	// the pre-checkpoint defer can safely own cleanup of both names.
	journalInstalled = true
	if err := maintenance.InstallWithHook(s.config.Dir, journal, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCPrepared); err != nil {
		return DataGCResult{}, err
	}
	if err := advanceDataGCJournal(s.config.Dir, &journal, 2, 0, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCCopying); err != nil {
		return DataGCResult{}, err
	}
	if err := session.relocate(ctx); err != nil {
		return DataGCResult{}, err
	}
	if err := advanceDataGCJournal(s.config.Dir, &journal, 3, 0, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCRelocations); err != nil {
		return DataGCResult{}, err
	}
	var checkpointManifest storeformat.Manifest
	if err := s.checkpointLocked(ctx, journal.Generation, &checkpointManifest, s.admitDataGCCheckpoint); err != nil {
		if errors.Is(err, base.ErrInsufficientSpace) {
			s.metrics.GCInsufficientSpace()
		}
		return DataGCResult{}, err
	}
	// checkpointLocked returned only after Manifest publication. From here,
	// validation or journal-publication errors are recovery-required failures;
	// the operation must never remove its ownership journal in-process.
	checkpointDurable = true
	if err := validateDataGCCheckpoint(checkpointManifest, session); err != nil {
		return DataGCResult{}, err
	}
	if checkpointManifest.MaintenanceGeneration != journal.Generation {
		return DataGCResult{}, fmt.Errorf("data GC checkpoint maintenance generation: %w", base.ErrCorrupt)
	}
	// Mapping Segment rotation during the nested checkpoint extends the parent
	// journal with its file-set changes. Reload that durable version before
	// advancing the DataGC phase so those recovery records are never dropped.
	durableJournal, found, err := maintenance.Load(s.config.Dir)
	if err != nil {
		return DataGCResult{}, err
	}
	if !found || durableJournal.Generation != journal.Generation || durableJournal.StoreUUID != journal.StoreUUID ||
		durableJournal.OperationID != journal.OperationID || durableJournal.OperationType != storeformat.MaintenanceDataGC || durableJournal.Phase != 3 {
		return DataGCResult{}, fmt.Errorf("data GC checkpoint journal identity: %w", base.ErrCorrupt)
	}
	journal = durableJournal
	// From this point the durable Manifest proves that no Mapping targets the
	// source. Failure to publish phase 4 must fail closed; fresh Open infers the
	// boundary from MaintenanceGeneration and resumes instead of rolling back.
	if err := advanceDataGCJournal(s.config.Dir, &journal, 4, checkpointManifest.Generation, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCCheckpoint); err != nil {
		return DataGCResult{}, err
	}
	sourceID := base.DataSegmentID(session.source.FileID)
	if err := s.segments.RetireCleaning(sourceID); err != nil {
		return DataGCResult{}, err
	}
	session.cleaning = false
	if err := advanceDataGCJournal(s.config.Dir, &journal, 5, journal.NewManifestGeneration, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCRetired); err != nil {
		return DataGCResult{}, err
	}
	if err := s.segments.WaitForNoReaders(ctx, sourceID); err != nil {
		return DataGCResult{}, err
	}
	removedManifest, err := s.removeDataGCSourceFromManifest(session, journal.Generation)
	if err != nil {
		return DataGCResult{}, err
	}
	s.mu.Lock()
	s.manifest = removedManifest
	s.mu.Unlock()
	if err := failpoint.Hit(s.hook, pointDataGCManifestRemoved); err != nil {
		return DataGCResult{}, err
	}
	sealed, err := s.segments.DetachRetired(sourceID)
	if err != nil {
		return DataGCResult{}, err
	}
	if err := sealed.Close(); err != nil {
		return DataGCResult{}, err
	}
	trashPath := dataGCTrashPath(s.config.Dir, operationID, sourceID)
	if err := os.Rename(dataGCSealedPath(s.config.Dir, sourceID), trashPath); err != nil {
		return DataGCResult{}, err
	}
	if err := maintenance.SyncDirectory(filepath.Join(s.config.Dir, "data")); err != nil {
		return DataGCResult{}, err
	}
	if err := maintenance.SyncDirectory(filepath.Join(s.config.Dir, "trash")); err != nil {
		return DataGCResult{}, err
	}
	if err := advanceDataGCJournal(s.config.Dir, &journal, 6, journal.NewManifestGeneration, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCTrashed); err != nil {
		return DataGCResult{}, err
	}
	if err := os.Remove(trashPath); err != nil {
		return DataGCResult{}, err
	}
	if err := maintenance.SyncDirectory(filepath.Join(s.config.Dir, "trash")); err != nil {
		return DataGCResult{}, err
	}
	if err := advanceDataGCJournal(s.config.Dir, &journal, 7, journal.NewManifestGeneration, s.hook); err != nil {
		return DataGCResult{}, err
	}
	if err := failpoint.Hit(s.hook, pointDataGCDeleted); err != nil {
		return DataGCResult{}, err
	}
	if err := maintenance.RemoveWithHook(s.config.Dir, s.hook); err != nil {
		return DataGCResult{}, err
	}
	sourceBytes, err := base.AddUint64(session.source.ValidEnd, storeformat.SegmentFooterSize)
	if err != nil {
		return DataGCResult{}, err
	}
	return DataGCResult{
		SourceSegmentID: sourceID, SourceBytes: sourceBytes, CopiedBytes: session.copiedBytes,
		Relocated: session.relocated, Skipped: session.skipped,
	}, nil
}

func advanceDataGCJournal(root string, journal *storeformat.MaintenanceJournal, phase uint16, newManifestGeneration uint64, hook failpoint.Hook) error {
	next := *journal
	next.SourceFiles = append([]storeformat.JournalFileRef(nil), journal.SourceFiles...)
	next.DestinationFiles = append([]storeformat.JournalFileRef(nil), journal.DestinationFiles...)
	next.Phase = phase
	if newManifestGeneration != 0 {
		next.NewManifestGeneration = newManifestGeneration
	}
	if err := storeformat.ValidateMaintenanceTransition(*journal, next); err != nil {
		return err
	}
	if err := maintenance.InstallWithHook(root, next, hook); err != nil {
		return err
	}
	*journal = next
	return nil
}

func validateDataGCCheckpoint(manifest storeformat.Manifest, session *dataGCSession) error {
	sourceID := base.DataSegmentID(session.source.FileID)
	if manifest.MaintenanceGeneration == 0 || manifest.CoveredCommitSeq < session.lastCommit || manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq {
		return fmt.Errorf("data GC checkpoint boundary: %w", base.ErrCorrupt)
	}
	if manifest.ReplayStart.SegmentID() < sourceID ||
		(manifest.ReplayStart.SegmentID() == sourceID && uint64(manifest.ReplayStart.Offset()) < session.source.ValidEnd) {
		return fmt.Errorf("data GC replay boundary before source end: %w", base.ErrCorrupt)
	}
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == sourceID {
			return fmt.Errorf("data GC checkpoint still reports source live data: %w", base.ErrConflict)
		}
	}
	return nil
}

func (s *Store) removeDataGCSourceFromManifest(session *dataGCSession, maintenanceGeneration uint64) (storeformat.Manifest, error) {
	sourceID := base.DataSegmentID(session.source.FileID)
	return s.catalog.Install(0, func(next *storeformat.Manifest) error {
		if next.MaintenanceGeneration != maintenanceGeneration || next.CoveredCommitSeq < session.lastCommit {
			return base.ErrConflict
		}
		found := false
		kept := next.SealedDataSegments[:0]
		for _, summary := range next.SealedDataSegments {
			if base.DataSegmentID(summary.FileID) == sourceID {
				if summary != session.source {
					return fmt.Errorf("data GC source summary changed: %w", base.ErrCorrupt)
				}
				found = true
				continue
			}
			kept = append(kept, summary)
		}
		if !found {
			return fmt.Errorf("data GC source missing before manifest removal: %w", base.ErrCorrupt)
		}
		next.SealedDataSegments = append([]storeformat.FileSummary(nil), kept...)
		for _, stat := range next.SegmentStats {
			if stat.SegmentID == sourceID {
				return fmt.Errorf("data GC source remains live at deletion: %w", base.ErrCorrupt)
			}
		}
		return nil
	})
}

func dataGCSealedPath(root string, id base.DataSegmentID) string {
	return filepath.Join(root, "data", segment.SealedDataFileName(id))
}

func dataGCTrashPath(root string, operationID [16]byte, id base.DataSegmentID) string {
	return filepath.Join(root, "trash", fmt.Sprintf("DATA-%08d.%x.trash", id, operationID))
}

func (s *Store) resumeDataGC() error {
	journal, found, err := maintenance.Load(s.config.Dir)
	if err != nil || !found {
		return err
	}
	if journal.OperationType != storeformat.MaintenanceDataGC {
		return nil
	}
	if journal.StoreUUID != s.catalog.Snapshot().StoreUUID {
		return fmt.Errorf("data GC recovery journal identity: %w", base.ErrCorrupt)
	}
	sourceRef, err := dataGCSourceRef(journal)
	if err != nil {
		return err
	}
	// Prepared/Copying/RelocationsDurable have not removed the source from the
	// Manifest. Replayed relocation seals are safe, so abandoning and retrying
	// the cleaning work is deterministic and keeps Open available even if the
	// nested checkpoint previously ran out of Mapping space.
	if journal.Phase <= 3 {
		current := s.catalog.Snapshot()
		if current.MaintenanceGeneration < journal.Generation {
			return maintenance.Remove(s.config.Dir)
		}
		if current.MaintenanceGeneration != journal.Generation || journal.Phase != 3 {
			return fmt.Errorf("data GC recovery checkpoint phase: %w", base.ErrCorrupt)
		}
		sourceID := base.DataSegmentID(sourceRef.FileID)
		for _, stat := range current.SegmentStats {
			if stat.SegmentID == sourceID {
				// A nested Mapping rotation can publish the parent maintenance
				// generation while the DataGC journal remains at phase 3. The
				// still-live source statistic proves its checkpoint did not run.
				return maintenance.Remove(s.config.Dir)
			}
		}
		// The checkpoint Manifest won the race with phase-4 journal publication.
		// Reconstruct the monotonic transition from durable Manifest evidence.
		if err := validateRecoveredDataGCCheckpoint(current, journal, sourceID); err != nil {
			return err
		}
		if err := advanceDataGCJournal(s.config.Dir, &journal, 4, current.Generation, nil); err != nil {
			return err
		}
	}
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.ops.RLock()
	defer s.ops.RUnlock()
	return s.resumeDataGCLocked(context.Background(), journal)
}

func (s *Store) resumeDataGCLocked(ctx context.Context, journal storeformat.MaintenanceJournal) error {
	ref, err := dataGCSourceRef(journal)
	if err != nil {
		return err
	}
	source := storeformat.FileSummary{FileID: ref.FileID, ValidEnd: ref.ValidEnd, FirstSeq: ref.FirstSeq, LastSeq: ref.LastSeq}
	sourceID := base.DataSegmentID(source.FileID)
	session := &dataGCSession{store: s, source: source}
	retiredInRuntime := false
	current := s.catalog.Snapshot()
	if current.MaintenanceGeneration != journal.Generation || current.Generation < journal.NewManifestGeneration {
		return fmt.Errorf("data GC recovery manifest generation: %w", base.ErrCorrupt)
	}
	if err := validateRecoveredDataGCCheckpoint(current, journal, sourceID); err != nil {
		return err
	}
	if journal.Phase == 4 {
		if !manifestHasDataSegment(current, sourceID) {
			return fmt.Errorf("data GC source missing at checkpoint phase: %w", base.ErrCorrupt)
		}
		if err := s.segments.BeginCleaning(sourceID); err != nil {
			return err
		}
		session.cleaning = true
		if err := session.validateSourceEmpty(ctx); err != nil {
			_ = session.cancel()
			return err
		}
		if err := s.segments.RetireCleaning(sourceID); err != nil {
			return err
		}
		session.cleaning = false
		retiredInRuntime = true
		if err := advanceDataGCJournal(s.config.Dir, &journal, 5, journal.NewManifestGeneration, nil); err != nil {
			return err
		}
	}
	if journal.Phase == 5 {
		current = s.catalog.Snapshot()
		if manifestHasDataSegment(current, sourceID) {
			if !retiredInRuntime {
				if err := s.segments.BeginCleaning(sourceID); err != nil {
					return err
				}
				session.cleaning = true
				if err := session.validateSourceEmpty(ctx); err != nil {
					_ = session.cancel()
					return err
				}
				if err := s.segments.RetireCleaning(sourceID); err != nil {
					return err
				}
				session.cleaning = false
				retiredInRuntime = true
			}
			if err := s.segments.WaitForNoReaders(ctx, sourceID); err != nil {
				return err
			}
			removed, err := s.removeDataGCSourceFromManifest(session, journal.Generation)
			if err != nil {
				return err
			}
			s.mu.Lock()
			s.manifest = removed
			s.mu.Unlock()
			sealed, err := s.segments.DetachRetired(sourceID)
			if err != nil {
				return err
			}
			if err := sealed.Close(); err != nil {
				return err
			}
		}
		if err := ensureDataGCTrashed(s.config.Dir, journal.OperationID, sourceID); err != nil {
			return err
		}
		if err := advanceDataGCJournal(s.config.Dir, &journal, 6, journal.NewManifestGeneration, nil); err != nil {
			return err
		}
	}
	if journal.Phase == 6 {
		if manifestHasDataSegment(s.catalog.Snapshot(), sourceID) {
			return fmt.Errorf("data GC source remains in manifest at trash phase: %w", base.ErrCorrupt)
		}
		trashPath := dataGCTrashPath(s.config.Dir, journal.OperationID, sourceID)
		if err := os.Remove(trashPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := maintenance.SyncDirectory(filepath.Join(s.config.Dir, "trash")); err != nil {
			return err
		}
		if err := advanceDataGCJournal(s.config.Dir, &journal, 7, journal.NewManifestGeneration, nil); err != nil {
			return err
		}
	}
	if journal.Phase != 7 {
		return fmt.Errorf("data GC recovery phase %d: %w", journal.Phase, base.ErrCorrupt)
	}
	return maintenance.Remove(s.config.Dir)
}

func validateRecoveredDataGCCheckpoint(manifest storeformat.Manifest, journal storeformat.MaintenanceJournal, sourceID base.DataSegmentID) error {
	if manifest.MaintenanceGeneration != journal.Generation || manifest.Generation < journal.NewManifestGeneration || manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq {
		return fmt.Errorf("recovered data GC checkpoint identity: %w", base.ErrCorrupt)
	}
	source, err := dataGCSourceRef(journal)
	if err != nil {
		return err
	}
	if manifest.ReplayStart.SegmentID() < sourceID ||
		(manifest.ReplayStart.SegmentID() == sourceID && uint64(manifest.ReplayStart.Offset()) < source.ValidEnd) {
		return fmt.Errorf("recovered data GC replay boundary before source: %w", base.ErrCorrupt)
	}
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == sourceID {
			return fmt.Errorf("recovered data GC source is still live: %w", base.ErrCorrupt)
		}
	}
	return nil
}

func dataGCSourceRef(journal storeformat.MaintenanceJournal) (storeformat.JournalFileRef, error) {
	var source storeformat.JournalFileRef
	for _, ref := range journal.SourceFiles {
		if ref.Kind != storeformat.FileKindData {
			continue
		}
		if source.FileID != 0 || ref.State != storeformat.FileStateSealed {
			return storeformat.JournalFileRef{}, fmt.Errorf("data GC source journal refs: %w", base.ErrCorrupt)
		}
		source = ref
	}
	if source.FileID == 0 {
		return storeformat.JournalFileRef{}, fmt.Errorf("data GC source journal ref missing: %w", base.ErrCorrupt)
	}
	return source, nil
}

func manifestHasDataSegment(manifest storeformat.Manifest, id base.DataSegmentID) bool {
	for _, summary := range manifest.SealedDataSegments {
		if base.DataSegmentID(summary.FileID) == id {
			return true
		}
	}
	return false
}

func ensureDataGCTrashed(root string, operationID [16]byte, id base.DataSegmentID) error {
	source, trash := dataGCSealedPath(root, id), dataGCTrashPath(root, operationID, id)
	_, sourceErr := os.Lstat(source)
	_, trashErr := os.Lstat(trash)
	if sourceErr == nil && trashErr == nil {
		return fmt.Errorf("data GC source and trash both exist: %w", base.ErrCorrupt)
	}
	if sourceErr == nil {
		if err := os.Rename(source, trash); err != nil {
			return err
		}
	} else if !errors.Is(sourceErr, os.ErrNotExist) {
		return sourceErr
	} else if trashErr != nil {
		if errors.Is(trashErr, os.ErrNotExist) {
			return fmt.Errorf("data GC source and trash both missing: %w", base.ErrCorrupt)
		}
		return trashErr
	}
	if err := maintenance.SyncDirectory(filepath.Join(root, "data")); err != nil {
		return err
	}
	return maintenance.SyncDirectory(filepath.Join(root, "trash"))
}

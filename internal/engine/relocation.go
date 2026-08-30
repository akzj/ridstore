package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

var errCheckpointStatsStale = errors.New("checkpoint stats still count source segment")

// SegmentRelocationResult describes only the copy-and-CAS phase of data GC.
// It does not imply checkpoint coverage or authorize Segment retirement.
type SegmentRelocationResult struct {
	ScannedRecords      uint64
	PutRecords          uint64
	LiveCandidates      uint64
	CopiedRecords       uint64
	CopiedValueBytes    uint64
	CopiedPhysicalBytes uint64
	Applied             uint64
	Skipped             uint64
	FirstCommitSeq      model.CommitSeq
	LastCommitSeq       model.CommitSeq
}

// SegmentRetirementProof captures the durable checkpoint at which source was
// proven to have no current Mapping or open-Batch references. It is not an
// authorization to unlink by itself: the eventual retire operation must
// consume and revalidate it while holding the maintenance gate.
type SegmentRetirementProof struct {
	Source            recordlog.SegmentSummary
	CatalogGeneration uint64
	CoveredCommitSeq  model.CommitSeq
	ReplayStart       recordlog.LogPos
}

type SegmentCompactionResult struct {
	Relocation SegmentRelocationResult
	Proof      SegmentRetirementProof
	Proofs     []SegmentRetirementProof
	Outputs    []recordlog.SegmentSummary
}

type NextSegmentCompactionResult struct {
	Candidate  SegmentCompactionCandidate
	Compaction SegmentCompactionResult
}

type copiedRecord struct {
	id         model.ID
	oldAddr    recordlog.VAddr
	newRef     recordlog.RecordRef
	valueBytes uint64
}

// RelocateSegment scans one sealed source Segment and copies records that are
// live at observation time. Publication is a physical-address CAS through the
// unique Coordinator, so a concurrent user update wins and leaves only an
// unreachable copy for later collection.
//
// Successful return does not make source safe to delete. A later GC phase must
// checkpoint the relocation CommitSeqs and prove exact liveness before retire.
func (s *Store) RelocateSegment(ctx context.Context, source recordlog.SegmentID) (SegmentRelocationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentRelocationResult{}, err
	}
	if source == 0 {
		return SegmentRelocationResult{}, base.ErrInvalidConfig
	}
	if err := s.beginOperation(); err != nil {
		return SegmentRelocationResult{}, err
	}
	defer s.endOperation()

	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	rate := s.gcBytesPerSecond.Load()
	return s.relocateSegment(ctx, source, rate)
}

// PrepareSegmentRetirement relocates current live records, checkpoints every
// resulting relocation, then proves that the source cannot be reintroduced by
// an already-open Batch. Data operations remain concurrent with the proof.
func (s *Store) PrepareSegmentRetirement(ctx context.Context, source recordlog.SegmentID) (SegmentRetirementProof, SegmentRelocationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentRetirementProof{}, SegmentRelocationResult{}, err
	}
	if source == 0 {
		return SegmentRetirementProof{}, SegmentRelocationResult{}, base.ErrInvalidConfig
	}
	if err := s.beginOperation(); err != nil {
		return SegmentRetirementProof{}, SegmentRelocationResult{}, err
	}
	defer s.endOperation()

	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	rate := s.gcBytesPerSecond.Load()
	relocated, err := s.relocateSegment(ctx, source, rate)
	if err != nil {
		return SegmentRetirementProof{}, relocated, err
	}
	proof, err := s.checkpointAndProveRetirement(ctx, source, relocated.LastCommitSeq)
	return proof, relocated, err
}

// CompactSegment executes the complete logical and physical retirement under
// the Engine's maintenance gate. A durable marker is installed immediately
// before Catalog removal so Open can finish or roll back any interrupted
// physical cleanup deterministically.
func (s *Store) CompactSegment(ctx context.Context, source recordlog.SegmentID) (SegmentCompactionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentCompactionResult{}, err
	}
	if source == 0 {
		return SegmentCompactionResult{}, base.ErrInvalidConfig
	}
	if err := s.beginOperation(); err != nil {
		return SegmentCompactionResult{}, err
	}
	defer s.endOperation()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	rate := s.gcBytesPerSecond.Load()
	s.metrics.gcStarted.Add(1)
	started := time.Now()
	manifest := s.catalog.Snapshot()
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool { return manifest.SealedDataSegments[i].SegmentID >= source })
	if index == len(manifest.SealedDataSegments) || manifest.SealedDataSegments[index].SegmentID != source || recordlog.IsCompactionSegment(source) {
		return SegmentCompactionResult{}, recordlog.ErrSegmentMissing
	}
	result, err := s.compactSegmentsLocked(ctx, []recordlog.SegmentSummary{manifest.SealedDataSegments[index]}, rate)
	s.recordCompactionMetrics(started, result, err)
	return result, err
}

// CompactNextSegment checkpoints current Mapping state, selects at most one
// checkpoint-safe candidate, and runs the full retirement protocol. A false
// found result means no Segment passed the policy and open-Batch gates.
func (s *Store) CompactNextSegment(ctx context.Context, policy CompactionPolicy) (NextSegmentCompactionResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return NextSegmentCompactionResult{}, false, err
	}
	policy = normalizeCompactionPolicy(policy)
	if policy.MinReclaimableRatioBasis > compactionRatioScale {
		return NextSegmentCompactionResult{}, false, base.ErrInvalidConfig
	}
	if err := s.beginOperation(); err != nil {
		return NextSegmentCompactionResult{}, false, err
	}
	defer s.endOperation()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	rate := s.gcBytesPerSecond.Load()
	if err := s.checkpoint(ctx, true); err != nil {
		return NextSegmentCompactionResult{}, false, fmt.Errorf("checkpoint before candidate selection: %w", err)
	}
	manifest := s.catalog.Snapshot()
	excluded, err := s.openBatchSegmentReferences(manifest.SealedDataSegments)
	if err != nil {
		return NextSegmentCompactionResult{}, false, fmt.Errorf("collect open batch segment references: %w", err)
	}
	now := time.Now()
	if s.gcNow != nil {
		now = s.gcNow()
	}
	candidate, found, err := selectCompactionCandidate(manifest, policy, excluded, func(id recordlog.SegmentID) (gcStabilityView, bool) {
		return s.gcStability.view(id, now, policy)
	})
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return NextSegmentCompactionResult{}, false, err
	}
	if !found {
		s.metrics.gcNoCandidate.Add(1)
		return NextSegmentCompactionResult{}, false, nil
	}
	s.metrics.gcStarted.Add(1)
	started := time.Now()
	compaction, err := s.compactSegmentsLocked(ctx, candidate.Sources, rate)
	s.recordCompactionMetrics(started, compaction, err)
	if err != nil {
		err = fmt.Errorf("compact segment %d: %w", candidate.Source.SegmentID, err)
	}
	return NextSegmentCompactionResult{Candidate: candidate, Compaction: compaction}, true, err
}

// compactSegmentsLocked rewrites a bounded adjacent run into immutable
// high-namespace output segments. User appends continue on the normal active
// segment throughout copying and CAS publication.
func (s *Store) compactSegmentsLocked(ctx context.Context, inputs []recordlog.SegmentSummary, rate uint64) (SegmentCompactionResult, error) {
	var result SegmentCompactionResult
	if len(inputs) == 0 || s.catalog == nil || s.maintenance == nil {
		return result, base.ErrInvalidConfig
	}
	manifest := s.catalog.Snapshot()
	for _, input := range inputs {
		if !containsSealedSegment(manifest, input) {
			return result, recordlog.ErrSegmentMissing
		}
	}
	space, err := s.reserveGCCopies(ctx, manifest, inputs)
	if err != nil {
		return result, err
	}
	defer space.complete(false)

	// One output slot per input is a strict upper bound: repacking only live
	// records cannot require more segments than their original sealed inputs.
	reserved, outputIDs, err := s.reserveCompactionOutputIDs(inputs, uint32(len(inputs)))
	if err != nil {
		return result, err
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID,
		LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: append([]recordlog.SegmentSummary(nil), inputs...), OutputIDs: outputIDs}
	if err := compactionstate.Install(s.root, state); err != nil {
		return result, errors.Join(base.ErrRecoveryRequired, err)
	}

	writer, err := s.maintenance.NewCompactionWriter(outputIDs)
	if err != nil {
		return result, s.compactionFailure(err)
	}
	pending := make([]copiedRecord, 0)
	for _, input := range inputs {
		err = s.maintenance.ScanSegment(ctx, input.SegmentID, func(scanned recordlog.AppendResult, payload []byte) error {
			result.Relocation.ScannedRecords++
			typ, decodeErr := recordcodec.TypeOf(payload)
			if decodeErr != nil {
				return errors.Join(base.ErrCorrupt, decodeErr)
			}
			if typ != recordcodec.RecordTypePut {
				return nil
			}
			result.Relocation.PutRecords++
			put, decodeErr := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
			if decodeErr != nil {
				return errors.Join(base.ErrCorrupt, decodeErr)
			}
			current, exists, lookupErr := s.mapping.LookupRef(put.RecordID)
			if lookupErr != nil {
				return lookupErr
			}
			if !exists || current.Addr != scanned.Addr {
				return nil
			}
			result.Relocation.LiveCandidates++
			copied, appendErr := writer.Append(ctx, payload)
			if appendErr != nil {
				return appendErr
			}
			ref, refErr := copied.Ref()
			if refErr != nil {
				return errors.Join(base.ErrCorrupt, refErr)
			}
			pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: scanned.Addr, newRef: ref, valueBytes: uint64(len(put.Value))})
			physical := uint64(copied.End.Offset - copied.Addr.Offset())
			result.Relocation.CopiedRecords++
			result.Relocation.CopiedPhysicalBytes += physical
			result.Relocation.CopiedValueBytes += uint64(len(put.Value))
			return nil
		})
		if err != nil {
			return result, s.compactionFailure(err)
		}
	}
	outputs, err := writer.Finish()
	if err != nil {
		return result, s.compactionFailure(err)
	}
	state.Outputs = outputs
	result.Outputs = append([]recordlog.SegmentSummary(nil), outputs...)
	if len(outputs) != 0 {
		if _, err = s.installCompactionOutputs(outputs); err != nil {
			return result, s.compactionFailure(err)
		}
		if err = s.maintenance.RegisterCompactionOutputs(outputs); err != nil {
			return result, s.compactionFailure(err)
		}
	}
	state.Phase = compactionstate.PhaseOutputsPublished
	if err = compactionstate.Update(s.root, state); err != nil {
		return result, s.compactionFailure(err)
	}

	if err = s.publishCopiedRecords(ctx, pending, &result.Relocation, rate); err != nil {
		return result, s.compactionFailure(err)
	}
	proofs, err := s.checkpointAndProveRetirements(ctx, inputs, result.Relocation.LastCommitSeq)
	if err != nil {
		return result, s.compactionFailure(err)
	}
	result.Proofs = proofs
	if len(proofs) != 0 {
		result.Proof = proofs[0]
	}
	installed, err := s.installCompactionRetirement(inputs)
	if err != nil {
		return result, s.compactionFailure(err)
	}
	state.Phase = compactionstate.PhaseInputsRetired
	if err = compactionstate.Update(s.root, state); err != nil {
		return result, s.compactionFailure(err)
	}
	if err = s.maintenance.FinalizeCompactionRetirement(ctx, inputs, installed.Generation); err != nil {
		return result, s.compactionFailure(err)
	}
	if err = compactionstate.Remove(s.root); err != nil {
		return result, s.compactionFailure(err)
	}
	space.complete(true)
	return result, nil
}

func (s *Store) reserveCompactionOutputIDs(inputs []recordlog.SegmentSummary, count uint32) (storecatalog.Manifest, []recordlog.SegmentID, error) {
	for attempt := 0; attempt < 32; attempt++ {
		current := s.catalog.Snapshot()
		for _, input := range inputs {
			if !containsSealedSegment(current, input) {
				return storecatalog.Manifest{}, nil, recordlog.ErrSegmentMissing
			}
		}
		installed, ids, err := s.catalog.ReserveCompactionSegments(current.Generation, count)
		if errors.Is(err, storecatalog.ErrConflict) {
			continue
		}
		return installed, ids, err
	}
	return storecatalog.Manifest{}, nil, base.ErrConflict
}

func (s *Store) installCompactionOutputs(outputs []recordlog.SegmentSummary) (storecatalog.Manifest, error) {
	for attempt := 0; attempt < 32; attempt++ {
		current := s.catalog.Snapshot()
		installed, err := s.catalog.InstallCompactionOutputs(current.Generation, outputs)
		if errors.Is(err, storecatalog.ErrConflict) {
			continue
		}
		return installed, err
	}
	return storecatalog.Manifest{}, base.ErrConflict
}

func (s *Store) installCompactionRetirement(inputs []recordlog.SegmentSummary) (storecatalog.Manifest, error) {
	for attempt := 0; attempt < 32; attempt++ {
		current := s.catalog.Snapshot()
		installed, err := s.catalog.InstallDataCompaction(current.Generation, inputs, current.CoveredCommitSeq, current.ReplayStart)
		if errors.Is(err, storecatalog.ErrConflict) {
			continue
		}
		return installed, err
	}
	return storecatalog.Manifest{}, base.ErrConflict
}

func (s *Store) compactionFailure(err error) error {
	if err == nil {
		return nil
	}
	result := errors.Join(base.ErrRecoveryRequired, err)
	s.setFault(result)
	return result
}

func (s *Store) publishCopiedRecords(ctx context.Context, copied []copiedRecord, result *SegmentRelocationResult, rate uint64) error {
	pacer := s.newGCPacer(rate)
	for len(copied) != 0 {
		count, bytes := 0, uint64(0)
		maxCount := min(len(copied), int(min(s.limits.MaxBatchMutations, s.maxRelocationMutations)))
		maxBytes := min(s.limits.MaxBatchBytes, s.maxRelocationBytes)
		for count < maxCount && (count == 0 || copied[count].valueBytes <= maxBytes-bytes) {
			bytes += copied[count].valueBytes
			count++
		}
		if count == 0 {
			return base.ErrBatchTooLarge
		}
		batch := copied[:count]
		sort.Slice(batch, func(i, j int) bool { return batch[i].id < batch[j].id })
		changes := make([]mapping.Change, len(batch))
		var physical uint64
		for i, item := range batch {
			changes[i] = mapping.Change{RecordID: item.id, ExpectedOldAddr: item.oldAddr, NewRef: item.newRef, Operation: mapping.OperationRelocate}
			physical += uint64(item.newRef.PhysicalSize)
		}
		rawBatchID, err := s.batches.Allocate(ctx)
		if err != nil {
			return err
		}
		published, err := s.commits.Relocate(ctx, coordinator.Relocation{BatchID: model.BatchID(rawBatchID), Changes: changes, LogicalPayloadBytes: bytes})
		if err != nil {
			return err
		}
		if result.FirstCommitSeq == 0 {
			result.FirstCommitSeq = published.CommitSeq
		}
		result.LastCommitSeq = published.CommitSeq
		result.Applied += uint64(published.Applied)
		result.Skipped += uint64(published.Skipped)
		waited, err := pacer.pace(ctx, physical)
		if waited > 0 {
			s.metrics.gcThrottledNanos.Add(uint64(waited))
		}
		if err != nil {
			return err
		}
		copied = copied[count:]
	}
	return nil
}

func (s *Store) checkpointAndProveRetirements(ctx context.Context, inputs []recordlog.SegmentSummary, end model.CommitSeq) ([]SegmentRetirementProof, error) {
	const maxAttempts = 32
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := s.checkpoint(ctx, true); err != nil {
			return nil, err
		}
		proofs := make([]SegmentRetirementProof, 0, len(inputs))
		stale := false
		for _, input := range inputs {
			proof, err := s.proveSegmentRetirement(ctx, input.SegmentID, end)
			if errors.Is(err, errCheckpointStatsStale) {
				stale = true
				break
			}
			if err != nil {
				return nil, err
			}
			proofs = append(proofs, proof)
		}
		if !stale {
			return proofs, nil
		}
	}
	return nil, errors.Join(base.ErrConflict, errCheckpointStatsStale)
}

func (s *Store) recordCompactionMetrics(started time.Time, result SegmentCompactionResult, err error) {
	s.metrics.gcDurationNanos.Add(uint64(time.Since(started)))
	if err != nil {
		s.metrics.gcFailed.Add(1)
		return
	}
	s.metrics.gcCompleted.Add(1)
	s.metrics.gcCopiedBytes.Add(result.Relocation.CopiedPhysicalBytes)
	s.metrics.gcRelocated.Add(result.Relocation.Applied)
	s.metrics.gcSkipped.Add(result.Relocation.Skipped)
	var sourceBytes uint64
	proofs := result.Proofs
	if len(proofs) == 0 && result.Proof.Source.SegmentID != 0 {
		proofs = []SegmentRetirementProof{result.Proof}
	}
	for _, proof := range proofs {
		if proof.Source.ValidEnd >= recordlog.SegmentHeaderSize {
			sourceBytes += uint64(proof.Source.ValidEnd - recordlog.SegmentHeaderSize)
		}
	}
	if sourceBytes >= result.Relocation.CopiedPhysicalBytes {
		s.metrics.gcReclaimedBytes.Add(sourceBytes - result.Relocation.CopiedPhysicalBytes)
	}
}

// compactSegmentLocked requires maintenanceMu.
func (s *Store) compactSegmentLocked(ctx context.Context, source recordlog.SegmentID, rate uint64) (SegmentCompactionResult, error) {

	relocated, err := s.relocateSegment(ctx, source, rate)
	result := SegmentCompactionResult{Relocation: relocated}
	if err != nil {
		return result, fmt.Errorf("relocate segment %d: %w", source, err)
	}
	proof, err := s.checkpointAndProveRetirement(ctx, source, relocated.LastCommitSeq)
	result.Proof = proof
	if err != nil {
		return result, fmt.Errorf("prove retirement of segment %d: %w", source, err)
	}
	manifest := s.catalog.Snapshot()
	if s.root == "" || manifest.Generation < proof.CatalogGeneration || manifest.RecordLogID == (recordlog.LogID{}) ||
		manifest.CoveredCommitSeq < proof.CoveredCommitSeq || manifest.ReplayStart.Compare(proof.ReplayStart) < 0 ||
		!containsSealedSegment(manifest, proof.Source) {
		return result, fmt.Errorf("retirement proof generation=%d current=%d: %w", proof.CatalogGeneration, manifest.Generation, base.ErrInvalidConfig)
	}
	state := maintstate.State{
		Operation: maintstate.DataRetire, StoreUUID: manifest.StoreUUID, LogID: manifest.RecordLogID,
		BaseGeneration: manifest.Generation, CoveredCommitSeq: manifest.CoveredCommitSeq,
		ReplayStart: manifest.ReplayStart, Source: proof.Source,
	}
	if err := maintstate.InstallWithFaultHook(s.root, state, s.maintenanceHook); err != nil {
		_, markerVisible, loadErr := maintstate.Load(s.root)
		if loadErr != nil || markerVisible {
			recoveryErr := errors.Join(base.ErrRecoveryRequired, err, loadErr)
			s.setFault(recoveryErr)
			return result, recoveryErr
		}
		return result, err
	}
	if err := s.maintenance.RetireSegment(ctx, source, manifest.Generation); err != nil {
		current := s.catalog.Snapshot()
		if current.Generation >= manifest.Generation && containsSealedSegment(current, proof.Source) {
			cleanupErr := maintstate.RemoveWithFaultHook(s.root, s.maintenanceHook)
			if cleanupErr == nil {
				return result, err
			}
			recoveryErr := errors.Join(base.ErrRecoveryRequired, err, cleanupErr)
			s.setFault(recoveryErr)
			return result, recoveryErr
		}
		recoveryErr := errors.Join(base.ErrRecoveryRequired, err)
		s.setFault(recoveryErr)
		return result, recoveryErr
	}
	if err := maintstate.RemoveWithFaultHook(s.root, s.maintenanceHook); err != nil {
		recoveryErr := errors.Join(base.ErrRecoveryRequired, err)
		s.setFault(recoveryErr)
		return result, recoveryErr
	}
	return result, nil
}

// checkpointAndProveRetirement retries only when a concurrent post-cut update
// has already removed the last runtime reference but the frozen checkpoint
// still conservatively counts it. Each retry is a normal online checkpoint;
// commits are paused only for the existing short cut barrier.
func (s *Store) checkpointAndProveRetirement(ctx context.Context, source recordlog.SegmentID, relocationEnd model.CommitSeq) (SegmentRetirementProof, error) {
	const maxAttempts = 32
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := s.checkpoint(ctx, true); err != nil {
			return SegmentRetirementProof{}, fmt.Errorf("checkpoint relocated segment %d: %w", source, err)
		}
		proof, err := s.proveSegmentRetirement(ctx, source, relocationEnd)
		if !errors.Is(err, errCheckpointStatsStale) {
			return proof, err
		}
	}
	return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errCheckpointStatsStale)
}

func (s *Store) openBatchSegmentReferences(sealed []recordlog.SegmentSummary) (map[recordlog.SegmentID]struct{}, error) {
	s.mutationFence.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.mutationFence.Unlock()
		return nil, base.ErrClosed
	}
	if s.fault != nil {
		s.mu.Unlock()
		s.mutationFence.Unlock()
		return nil, errors.Join(base.ErrReadOnly, s.fault)
	}
	open := make([]*Batch, 0, len(s.open))
	for _, batch := range s.open {
		open = append(open, batch)
	}
	s.mu.Unlock()
	s.mutationFence.Unlock()
	result := make(map[recordlog.SegmentID]struct{})
	for _, source := range sealed {
		for _, batch := range open {
			if batch.inner.ReferencesSegment(source.SegmentID) {
				result[source.SegmentID] = struct{}{}
				break
			}
		}
	}
	return result, nil
}

func containsSealedSegment(manifest storecatalog.Manifest, source recordlog.SegmentSummary) bool {
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool {
		return manifest.SealedDataSegments[i].SegmentID >= source.SegmentID
	})
	return index < len(manifest.SealedDataSegments) && manifest.SealedDataSegments[index] == source
}

func (s *Store) proveSegmentRetirement(ctx context.Context, source recordlog.SegmentID, relocationEnd model.CommitSeq) (SegmentRetirementProof, error) {
	s.mutationFence.Lock()
	s.mu.Lock()
	closed, fault := s.closed, s.fault
	if closed {
		s.mu.Unlock()
		s.mutationFence.Unlock()
		return SegmentRetirementProof{}, base.ErrClosed
	}
	if fault != nil {
		s.mu.Unlock()
		s.mutationFence.Unlock()
		return SegmentRetirementProof{}, errors.Join(base.ErrReadOnly, fault)
	}
	open := make([]*Batch, 0, len(s.open))
	for _, batch := range s.open {
		open = append(open, batch)
	}
	s.mu.Unlock()
	s.mutationFence.Unlock()
	for _, batch := range open {
		if batch.inner.ReferencesSegment(source) {
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errors.New("open batch references source segment"))
		}
	}
	if commitFault := s.commits.Fault(); commitFault != nil {
		return SegmentRetirementProof{}, errors.Join(base.ErrReadOnly, commitFault)
	}

	manifest := s.catalog.Snapshot()
	if manifest.CoveredCommitSeq < relocationEnd || manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq {
		err := errors.Join(base.ErrCorrupt, errors.New("checkpoint does not cover relocation"))
		s.setFault(err)
		return SegmentRetirementProof{}, err
	}
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool {
		return manifest.SealedDataSegments[i].SegmentID >= source
	})
	if index == len(manifest.SealedDataSegments) || manifest.SealedDataSegments[index].SegmentID != source {
		return SegmentRetirementProof{}, recordlog.ErrSegmentMissing
	}
	if !storecatalog.StatsKnownForSegment(manifest.ReplayStart, manifest.SealedDataSegments[index]) {
		return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errors.New("segment stats are not complete at checkpoint boundary"))
	}
	err := s.maintenance.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		if typ != recordcodec.RecordTypePut {
			return nil
		}
		put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		current, exists, err := s.mapping.Lookup(put.RecordID)
		if err != nil {
			return err
		}
		if exists && current == scanned.Addr {
			return errors.Join(base.ErrConflict, errors.New("mapping still references source segment"))
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return SegmentRetirementProof{}, err
	}
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == source && (stat.LiveBytes != 0 || stat.LiveRecords != 0) {
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errCheckpointStatsStale,
				fmt.Errorf("live_bytes=%d live_records=%d", stat.LiveBytes, stat.LiveRecords))
		}
	}
	return SegmentRetirementProof{
		Source: manifest.SealedDataSegments[index], CatalogGeneration: manifest.Generation,
		CoveredCommitSeq: manifest.CoveredCommitSeq, ReplayStart: manifest.ReplayStart,
	}, nil
}

// relocateSegment requires maintenanceMu. Keeping orchestration ownership at
// Store lets a later complete GC operation compose relocation, checkpoint and
// retirement without recursively acquiring the maintenance lock.
func (s *Store) relocateSegment(ctx context.Context, source recordlog.SegmentID, rate uint64) (SegmentRelocationResult, error) {
	s.mu.Lock()
	closed, fault := s.closed, s.fault
	s.mu.Unlock()
	if closed {
		return SegmentRelocationResult{}, base.ErrClosed
	}
	if fault != nil {
		return SegmentRelocationResult{}, errors.Join(base.ErrReadOnly, fault)
	}
	if commitFault := s.commits.Fault(); commitFault != nil {
		return SegmentRelocationResult{}, errors.Join(base.ErrReadOnly, commitFault)
	}
	if s.catalog == nil || s.maintenance == nil || s.maxRelocationMutations == 0 {
		return SegmentRelocationResult{}, base.ErrInvalidConfig
	}
	manifest := s.catalog.Snapshot()
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool {
		return manifest.SealedDataSegments[i].SegmentID >= source
	})
	if index == len(manifest.SealedDataSegments) || manifest.SealedDataSegments[index].SegmentID != source {
		return SegmentRelocationResult{}, recordlog.ErrSegmentMissing
	}
	space, err := s.reserveGCCopy(ctx, manifest, manifest.SealedDataSegments[index])
	if err != nil {
		return SegmentRelocationResult{}, err
	}
	defer space.complete(false)

	var result SegmentRelocationResult
	pending := make([]copiedRecord, 0, min(s.limits.MaxBatchMutations, s.maxRelocationMutations))
	var pendingBytes uint64
	var pendingPhysical uint64
	pacer := s.newGCPacer(rate)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		// Segment scan order is physical append order, while Mapping proposals
		// require changes to be strictly ordered by RecordID.
		sort.Slice(pending, func(i, j int) bool { return pending[i].id < pending[j].id })
		rawBatchID, err := s.batches.Allocate(ctx)
		if err != nil {
			return err
		}
		changes := make([]mapping.Change, len(pending))
		for i, copied := range pending {
			changes[i] = mapping.Change{
				RecordID: copied.id, ExpectedOldAddr: copied.oldAddr,
				NewRef: copied.newRef, Operation: mapping.OperationRelocate,
			}
		}
		published, err := s.commits.Relocate(ctx, coordinator.Relocation{
			BatchID: model.BatchID(rawBatchID), Changes: changes, LogicalPayloadBytes: pendingBytes,
		})
		if err != nil {
			return err
		}
		if result.FirstCommitSeq == 0 {
			result.FirstCommitSeq = published.CommitSeq
		}
		if published.CommitSeq <= result.LastCommitSeq {
			return errors.Join(base.ErrCorrupt, errors.New("non-monotonic relocation commit sequence"))
		}
		result.LastCommitSeq = published.CommitSeq
		result.Applied += uint64(published.Applied)
		result.Skipped += uint64(published.Skipped)
		waited, err := pacer.pace(ctx, pendingPhysical)
		if waited > 0 {
			s.metrics.gcThrottledNanos.Add(uint64(waited))
		}
		if err != nil {
			return err
		}
		pending = pending[:0]
		pendingBytes = 0
		pendingPhysical = 0
		return nil
	}

	err = s.maintenance.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
		result.ScannedRecords++
		typ, err := recordcodec.TypeOf(payload)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		if typ != recordcodec.RecordTypePut {
			return nil
		}
		result.PutRecords++
		put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		current, exists, err := s.mapping.Lookup(put.RecordID)
		if err != nil {
			return err
		}
		if !exists || current != scanned.Addr {
			return nil
		}
		result.LiveCandidates++
		valueBytes := uint64(len(put.Value))
		exceeds := func(limit uint64) bool { return valueBytes > limit || pendingBytes > limit-valueBytes }
		if len(pending) != 0 && (uint64(len(pending)) == s.limits.MaxBatchMutations ||
			uint64(len(pending)) == s.maxRelocationMutations || exceeds(s.limits.MaxBatchBytes) ||
			exceeds(s.maxRelocationBytes)) {
			if err := flush(); err != nil {
				return err
			}
		}
		copied, err := s.log.Append(ctx, payload, false)
		if err != nil {
			return err
		}
		physical, sizeErr := recordlog.PhysicalRecordSize(uint64(len(payload)))
		if sizeErr == nil {
			s.recordMeta.Remember(copied.Addr, put.RecordID, physical)
		}
		ref, err := copied.Ref()
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: scanned.Addr, newRef: ref, valueBytes: valueBytes})
		pendingBytes += valueBytes
		physicalCopied := uint64(copied.End.Offset - copied.Addr.Offset())
		if pendingPhysical > math.MaxUint64-physicalCopied {
			return base.ErrOverflow
		}
		pendingPhysical += physicalCopied
		result.CopiedRecords++
		physicalBytes := uint64(scanned.End.Offset - scanned.Addr.Offset())
		if result.CopiedPhysicalBytes > math.MaxUint64-physicalBytes {
			return base.ErrBatchTooLarge
		}
		result.CopiedPhysicalBytes += physicalBytes
		if result.CopiedValueBytes > math.MaxUint64-valueBytes {
			return base.ErrBatchTooLarge
		}
		result.CopiedValueBytes += valueBytes
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return result, err
	}
	space.complete(true)
	return result, nil
}

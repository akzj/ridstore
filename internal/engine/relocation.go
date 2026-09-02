package engine

import (
	"bytes"
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
	Source             recordlog.SegmentSummary
	CatalogGeneration  uint64
	CoveredCommitSeq   model.CommitSeq
	ReplayStart        recordlog.LogPos
	ManifestGeneration uint64
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

type openBatchCompactionRefs struct {
	refs    map[recordlog.VAddr]recordlog.RecordRef
	batches []*Batch
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

	if err := s.acquireDataMaintenance(ctx); err != nil {
		return SegmentRelocationResult{}, err
	}
	defer s.releaseDataMaintenance()
	rate := s.maintenance.gcBytesPerSecond.Load()
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

	if err := s.acquireDataMaintenance(ctx); err != nil {
		return SegmentRetirementProof{}, SegmentRelocationResult{}, err
	}
	defer s.releaseDataMaintenance()
	rate := s.maintenance.gcBytesPerSecond.Load()
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
	if err := s.acquireDataMaintenance(ctx); err != nil {
		return SegmentCompactionResult{}, err
	}
	defer s.releaseDataMaintenance()
	rate := s.maintenance.gcBytesPerSecond.Load()
	s.metrics.gcStarted.Add(1)
	started := time.Now()
	manifest := s.catalogSnapshot()
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool { return manifest.SealedDataSegments[i].SegmentID >= source })
	if index == len(manifest.SealedDataSegments) || manifest.SealedDataSegments[index].SegmentID != source || recordlog.IsCompactionSegment(source) {
		return SegmentCompactionResult{}, recordlog.ErrSegmentMissing
	}
	result, err := s.compactSegmentsLocked(ctx, []recordlog.SegmentSummary{manifest.SealedDataSegments[index]}, rate)
	s.recordCompactionMetrics(started, result, err)
	return result, err
}

// CompactNextSegment checkpoints current Mapping state, selects at most one
// checkpoint-safe candidate, and runs the full retirement protocol. Open Batch
// Put references are copied and redirected rather than excluding a candidate.
// A false found result means no Segment passed the policy gates.
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
	if err := s.acquireDataMaintenance(ctx); err != nil {
		return NextSegmentCompactionResult{}, false, err
	}
	defer s.releaseDataMaintenance()
	rate := s.maintenance.gcBytesPerSecond.Load()
	selectCandidate := func() (SegmentCompactionCandidate, bool, error) {
		published := s.PublishedState()
		if published == nil {
			return SegmentCompactionCandidate{}, false, base.ErrInvalidConfig
		}
		manifest := published.Manifest
		excluded, err := s.openBatchCompactionOutputReferences(manifest.SealedDataSegments)
		if err != nil {
			return SegmentCompactionCandidate{}, false, fmt.Errorf("collect open batch compaction-output references: %w", err)
		}
		now := time.Now()
		if s.maintenance.gcNow != nil {
			now = s.maintenance.gcNow()
		}
		return selectCompactionCandidate(manifest, policy, excluded, func(id recordlog.SegmentID) (gcStabilityView, bool) {
			return s.maintenance.gcStability.view(id, now, policy)
		})
	}
	// A durable checkpoint remains an exact scheduling snapshot until replaced.
	// Try it first so a successful GC normally pays only the post-relocation
	// checkpoint. Refresh only when no existing candidate is usable.
	candidate, found, err := selectCandidate()
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return NextSegmentCompactionResult{}, false, err
	}
	if !found {
		if err := s.checkpoint(ctx, true); err != nil {
			return NextSegmentCompactionResult{}, false, fmt.Errorf("checkpoint before candidate selection: %w", err)
		}
		candidate, found, err = selectCandidate()
		if err != nil {
			if errors.Is(err, base.ErrCorrupt) {
				s.setFault(err)
			}
			return NextSegmentCompactionResult{}, false, err
		}
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
	if len(inputs) == 0 || s.core.catalog == nil || s.core.compactionLog == nil {
		return result, base.ErrInvalidConfig
	}
	published := s.PublishedState()
	if published == nil {
		return result, base.ErrInvalidConfig
	}
	manifest := published.Manifest
	for _, input := range inputs {
		if !containsSealedSegment(manifest, input) {
			return result, recordlog.ErrSegmentMissing
		}
	}
	pending, err := s.snapshotOpenBatchCompactionRefs(inputs)
	if err != nil {
		return result, err
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
	if err := compactionstate.Install(s.core.root, state); err != nil {
		return result, errors.Join(base.ErrRecoveryRequired, err)
	}
	rollbackUnpublished := func(writer *recordlog.CompactionWriter, cause error) error {
		cleanupErr := errors.Join(writer.Abort(), s.core.compactionLog.RemoveUnpublishedCompactionFiles(outputIDs))
		if cleanupErr == nil {
			cleanupErr = compactionstate.Remove(s.core.root)
		}
		if cleanupErr != nil {
			return s.compactionFailure(errors.Join(cause, cleanupErr))
		}
		return cause
	}

	writer, err := s.core.compactionLog.NewCompactionWriter(outputIDs)
	if err != nil {
		return result, rollbackUnpublished(nil, err)
	}
	copyPacer := s.newGCPacer(rate)
	redirects := make(map[recordlog.VAddr]recordlog.RecordRef, len(pending.refs))
	pendingOrigins := make(map[recordlog.VAddr]recordlog.VAddr, len(pending.refs))
	var unpacedBytes uint64
	paceCopy := func(force bool) error {
		if unpacedBytes == 0 || !force && unpacedBytes < s.maintenance.maxRelocationBytes {
			return nil
		}
		waited, paceErr := copyPacer.pace(ctx, unpacedBytes)
		if waited > 0 {
			s.metrics.gcThrottledNanos.Add(uint64(waited))
		}
		unpacedBytes = 0
		return paceErr
	}
	for _, input := range inputs {
		err = s.core.compactionLog.ScanSegment(ctx, input.SegmentID, func(scanned recordlog.AppendResult, payload []byte) error {
			result.Relocation.ScannedRecords++
			typ, decodeErr := recordcodec.TypeOf(payload)
			if decodeErr != nil {
				return errors.Join(base.ErrCorrupt, decodeErr)
			}
			if typ != recordcodec.RecordTypePut {
				return nil
			}
			result.Relocation.PutRecords++
			put, decodeErr := recordcodec.DecodePut(payload, s.state.limits.MaxValueSize)
			if decodeErr != nil {
				return errors.Join(base.ErrCorrupt, decodeErr)
			}
			current, exists, lookupErr := s.core.mapping.LookupRef(put.RecordID)
			if lookupErr != nil {
				return lookupErr
			}
			pendingRef, pendingLive := pending.refs[scanned.Addr]
			if pendingLive {
				scannedRef, refErr := scanned.Ref()
				if refErr != nil || scannedRef != pendingRef {
					return errors.Join(base.ErrCorrupt, refErr)
				}
			}
			if (!exists || current.Addr != scanned.Addr) && !pendingLive {
				return nil
			}
			result.Relocation.LiveCandidates++
			copied, appendErr := writer.Append(ctx, payload)
			if appendErr != nil {
				return appendErr
			}
			physical := uint64(copied.End.Offset - copied.Addr.Offset())
			if result.Relocation.CopiedPhysicalBytes > math.MaxUint64-physical ||
				result.Relocation.CopiedValueBytes > math.MaxUint64-uint64(len(put.Value)) ||
				unpacedBytes > math.MaxUint64-physical {
				return base.ErrOverflow
			}
			result.Relocation.CopiedRecords++
			result.Relocation.CopiedPhysicalBytes += physical
			result.Relocation.CopiedValueBytes += uint64(len(put.Value))
			unpacedBytes += physical
			if pendingLive {
				newRef, refErr := copied.Ref()
				if refErr != nil {
					return errors.Join(base.ErrCorrupt, refErr)
				}
				redirects[scanned.Addr] = newRef
				pendingOrigins[newRef.Addr] = scanned.Addr
			}
			return paceCopy(false)
		})
		if err != nil {
			return result, rollbackUnpublished(writer, err)
		}
	}
	if err = paceCopy(true); err != nil {
		return result, rollbackUnpublished(writer, err)
	}
	outputs, err := writer.Finish()
	if err != nil {
		return result, rollbackUnpublished(writer, err)
	}
	state.Outputs = outputs
	result.Outputs = append([]recordlog.SegmentSummary(nil), outputs...)
	if len(outputs) != 0 {
		if _, err = s.installCompactionOutputs(outputs); err != nil {
			return result, s.compactionFailure(err)
		}
		if err = s.core.compactionLog.RegisterCompactionOutputs(outputs); err != nil {
			return result, s.compactionFailure(err)
		}
	}
	state.Phase = compactionstate.PhaseOutputsPublished
	if err = compactionstate.Update(s.core.root, state); err != nil {
		return result, s.compactionFailure(err)
	}

	if len(redirects) != len(pending.refs) {
		return result, s.compactionFailure(errors.Join(base.ErrCorrupt, errors.New("open batch compaction reference was not copied")))
	}
	if err = s.rewriteOpenBatchCompactionRefs(ctx, pending.batches, redirects); err != nil {
		return result, s.compactionFailure(err)
	}
	if err = s.publishCompactionOutputs(ctx, inputs, outputs, pendingOrigins, false, &result.Relocation); err != nil {
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
	installed, err := s.installCompactionRetirement(inputs, proofs)
	if err != nil {
		return result, s.compactionFailure(err)
	}
	state.Phase = compactionstate.PhaseInputsRetired
	if err = compactionstate.Update(s.core.root, state); err != nil {
		return result, s.compactionFailure(err)
	}
	if err = s.core.compactionLog.FinalizeCompactionRetirement(ctx, inputs, installed.Generation); err != nil {
		return result, s.compactionFailure(err)
	}
	if err = compactionstate.Remove(s.core.root); err != nil {
		return result, s.compactionFailure(err)
	}
	space.complete(true)
	return result, nil
}

func (s *Store) reserveCompactionOutputIDs(inputs []recordlog.SegmentSummary, count uint32) (storecatalog.Manifest, []recordlog.SegmentID, error) {
	for attempt := 0; attempt < 32; attempt++ {
		current := s.catalogSnapshot()
		for _, input := range inputs {
			if !containsSealedSegment(current, input) {
				return storecatalog.Manifest{}, nil, recordlog.ErrSegmentMissing
			}
		}
		installed, ids, err := s.core.publisher.ReserveCompactionSegments(current.Generation, count)
		if errors.Is(err, storecatalog.ErrConflict) {
			continue
		}
		return installed, ids, err
	}
	return storecatalog.Manifest{}, nil, base.ErrConflict
}

func (s *Store) installCompactionOutputs(outputs []recordlog.SegmentSummary) (storecatalog.Manifest, error) {
	for attempt := 0; attempt < 32; attempt++ {
		current := s.catalogSnapshot()
		installed, err := s.core.publisher.InstallCompactionOutputs(current.Generation, outputs)
		if errors.Is(err, storecatalog.ErrConflict) {
			continue
		}
		return installed, err
	}
	return storecatalog.Manifest{}, base.ErrConflict
}

func (s *Store) installCompactionRetirement(inputs []recordlog.SegmentSummary, proofs []SegmentRetirementProof) (storecatalog.Manifest, error) {
	manifest, err := s.validateRetirementProofs(inputs, proofs)
	if err != nil {
		return storecatalog.Manifest{}, err
	}
	installed, err := s.core.publisher.InstallDataCompaction(manifest.Generation, inputs, manifest.CoveredCommitSeq, manifest.ReplayStart)
	if errors.Is(err, storecatalog.ErrConflict) {
		err = errors.Join(base.ErrConflict, err)
	}
	return installed, err
}

func (s *Store) compactionFailure(err error) error {
	if err == nil {
		return nil
	}
	result := errors.Join(base.ErrRecoveryRequired, err)
	s.setFault(result)
	return result
}

func (s *Store) publishCopiedRecords(ctx context.Context, copied []copiedRecord, result *SegmentRelocationResult) error {
	for len(copied) != 0 {
		count, bytes := 0, uint64(0)
		maxCount := min(len(copied), int(min(s.state.limits.MaxBatchMutations, s.maintenance.maxRelocationMutations)))
		maxBytes := min(s.state.limits.MaxBatchBytes, s.maintenance.maxRelocationBytes)
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
		for i, item := range batch {
			changes[i] = mapping.Change{RecordID: item.id, ExpectedOldAddr: item.oldAddr, NewRef: item.newRef, Operation: mapping.OperationRelocate}
		}
		rawBatchID, err := s.core.batches.Allocate(ctx)
		if err != nil {
			return err
		}
		published, err := s.relocateWithBudgetRetry(ctx, coordinator.Relocation{BatchID: model.BatchID(rawBatchID), Changes: changes, LogicalPayloadBytes: bytes})
		if err != nil {
			return err
		}
		if result.FirstCommitSeq == 0 {
			result.FirstCommitSeq = published.CommitSeq
		}
		result.LastCommitSeq = published.CommitSeq
		result.Applied += uint64(published.Applied)
		result.Skipped += uint64(published.Skipped)
		copied = copied[count:]
	}
	return nil
}

// publishCompactionOutputs reconstructs relocation proposals from immutable
// output records in bounded batches. A current ref in one of the input
// segments is the exact CAS source; refs already moved by an earlier recovery
// attempt or by a user update need no proposal.
func (s *Store) publishCompactionOutputs(ctx context.Context, inputs, outputs []recordlog.SegmentSummary, pendingOrigins map[recordlog.VAddr]recordlog.VAddr, verifySourcePayload bool, result *SegmentRelocationResult) error {
	inputIDs := make(map[recordlog.SegmentID]struct{}, len(inputs))
	for _, input := range inputs {
		inputIDs[input.SegmentID] = struct{}{}
	}
	pendingSources := make(map[recordlog.VAddr]struct{}, len(pendingOrigins))
	for _, source := range pendingOrigins {
		pendingSources[source] = struct{}{}
	}
	capacity := int(min(s.state.limits.MaxBatchMutations, s.maintenance.maxRelocationMutations))
	if capacity == 0 {
		return base.ErrInvalidConfig
	}
	pending := make([]copiedRecord, 0, capacity)
	pendingIDs := make(map[model.ID]struct{}, capacity)
	var pendingBytes uint64
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := s.publishCopiedRecords(ctx, pending, result); err != nil {
			return err
		}
		pending = pending[:0]
		pendingBytes = 0
		clear(pendingIDs)
		return nil
	}
	for _, output := range outputs {
		if err := s.core.compactionLog.ScanSegment(ctx, output.SegmentID, func(scanned recordlog.AppendResult, payload []byte) error {
			typ, err := recordcodec.TypeOf(payload)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if typ != recordcodec.RecordTypePut {
				return errors.Join(base.ErrCorrupt, errors.New("non-put record in compaction output"))
			}
			put, err := recordcodec.DecodePut(payload, s.state.limits.MaxValueSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			current, exists, err := s.core.mapping.LookupRef(put.RecordID)
			if err != nil {
				return err
			}
			if !exists {
				result.Skipped++
				return nil
			}
			if _, source := inputIDs[current.Addr.SegmentID()]; !source {
				result.Skipped++
				return nil
			}
			ref, err := scanned.Ref()
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if origin, pendingCopy := pendingOrigins[scanned.Addr]; pendingCopy {
				if current.Addr != origin {
					result.Skipped++
					return nil
				}
			} else if _, currentIsPending := pendingSources[current.Addr]; currentIsPending {
				// The same RecordID may have both a committed source version and
				// several unpublished Batch versions in the input run. Only the
				// output copied from the exact pending source may relocate it.
				result.Skipped++
				return nil
			}
			if verifySourcePayload {
				sourcePayload, readErr := s.core.log.Read(ctx, current.Addr)
				if readErr != nil {
					return readErr
				}
				if !bytes.Equal(sourcePayload, payload) {
					result.Skipped++
					return nil
				}
			}
			if _, duplicate := pendingIDs[put.RecordID]; duplicate {
				result.Skipped++
				return nil
			}
			valueBytes := uint64(len(put.Value))
			exceedsBytes := valueBytes > s.maintenance.maxRelocationBytes || pendingBytes > s.maintenance.maxRelocationBytes-valueBytes
			if len(pending) != 0 && (len(pending) == capacity || exceedsBytes) {
				if err := flush(); err != nil {
					return err
				}
			}
			pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: current.Addr, newRef: ref, valueBytes: valueBytes})
			pendingIDs[put.RecordID] = struct{}{}
			pendingBytes += valueBytes
			return nil
		}); err != nil {
			return err
		}
	}
	return flush()
}

// snapshotOpenBatchCompactionRefs captures the only unpublished addresses that
// can still enter Mapping for sealed inputs. Put-like operations are drained so
// a Record append cannot be observed without its final Batch mutation.
func (s *Store) snapshotOpenBatchCompactionRefs(inputs []recordlog.SegmentSummary) (openBatchCompactionRefs, error) {
	inputIDs := make(map[recordlog.SegmentID]struct{}, len(inputs))
	for _, input := range inputs {
		inputIDs[input.SegmentID] = struct{}{}
	}
	s.mutationAdmission.writeLock()
	s.state.mu.Lock()
	if s.state.closed {
		s.state.mu.Unlock()
		s.mutationAdmission.writeUnlock()
		return openBatchCompactionRefs{}, base.ErrClosed
	}
	if s.state.fault != nil {
		fault := s.state.fault
		s.state.mu.Unlock()
		s.mutationAdmission.writeUnlock()
		return openBatchCompactionRefs{}, errors.Join(base.ErrReadOnly, fault)
	}
	open := make([]*Batch, 0, len(s.state.open))
	for _, batch := range s.state.open {
		open = append(open, batch)
	}
	s.state.mu.Unlock()
	// Taking the write side above drains Put-like operations that may have
	// appended to a now-sealed input but not yet installed their Batch mutation.
	// Once drained, later Put records can only enter the active Segment. Release
	// the global fence before inspecting individual Batches so the scan cost is
	// never added to foreground Put latency.
	s.mutationAdmission.writeUnlock()
	result := openBatchCompactionRefs{refs: make(map[recordlog.VAddr]recordlog.RecordRef)}
	for _, batch := range open {
		referenced := false
		for _, ref := range batch.inner.PendingPutRefs() {
			if _, ok := inputIDs[ref.Addr.SegmentID()]; !ok {
				continue
			}
			result.refs[ref.Addr] = ref
			referenced = true
		}
		if referenced {
			result.batches = append(result.batches, batch)
		}
	}
	return result, nil
}

// rewriteOpenBatchCompactionRefs establishes the publication boundary for
// unpublished Put addresses. The Coordinator first installs an address rewrite
// table after older requests, so already-prepared and concurrent Commit requests
// do not have to stop while still-open Batch mutations are rewritten. Release
// briefly fences admission only to enqueue the redirect-removal boundary.
func (s *Store) rewriteOpenBatchCompactionRefs(ctx context.Context, batches []*Batch, redirects map[recordlog.VAddr]recordlog.RecordRef) error {
	if len(redirects) == 0 {
		return nil
	}
	waitStarted := time.Now()
	installed, err := s.core.commits.InstallCommitRedirects(ctx, redirects)
	if err != nil {
		return err
	}
	s.metrics.gcCommitRedirects.Add(1)
	s.metrics.gcCommitRedirectWaitNanos.Add(uint64(time.Since(waitStarted)))
	var rewritten uint64
	for _, batch := range batches {
		rewritten += batch.inner.RewritePutRefs(redirects)
	}
	admissionNanos, err := installed.Release(ctx)
	s.metrics.gcCommitRedirectAdmissionNanos.Add(uint64(admissionNanos))
	s.metrics.gcOpenRefsRedirected.Add(rewritten)
	return err
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
			proof, err := s.proveCompactionRetirement(input.SegmentID, end)
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

// compactSegmentLocked is called while the scheduler owns the data
// maintenance slot.
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
	manifest, err := s.validateRetirementProofs([]recordlog.SegmentSummary{proof.Source}, []SegmentRetirementProof{proof})
	if err != nil {
		return result, err
	}
	state := maintstate.State{
		Operation: maintstate.DataRetire, StoreUUID: manifest.StoreUUID, LogID: manifest.RecordLogID,
		BaseGeneration: proof.ManifestGeneration, CoveredCommitSeq: proof.CoveredCommitSeq,
		ReplayStart: manifest.ReplayStart, Source: proof.Source,
	}
	if err := maintstate.InstallWithFaultHook(s.core.root, state, s.maintenance.stateHook); err != nil {
		_, markerVisible, loadErr := maintstate.Load(s.core.root)
		if loadErr != nil || markerVisible {
			recoveryErr := errors.Join(base.ErrRecoveryRequired, err, loadErr)
			s.setFault(recoveryErr)
			return result, recoveryErr
		}
		return result, err
	}
	installed, err := s.core.publisher.InstallDataRetire(proof.ManifestGeneration, storecatalog.DataRetire{
		Source: proof.Source, CoveredCommitSeq: proof.CoveredCommitSeq, ReplayStart: proof.ReplayStart,
	})
	if err != nil {
		if errors.Is(err, storecatalog.ErrConflict) {
			err = errors.Join(base.ErrConflict, err)
		}
		current := s.catalogSnapshot()
		if current.Generation >= manifest.Generation && containsSealedSegment(current, proof.Source) {
			cleanupErr := maintstate.RemoveWithFaultHook(s.core.root, s.maintenance.stateHook)
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
	if err := s.core.compactionLog.FinalizeCompactionRetirement(ctx, []recordlog.SegmentSummary{proof.Source}, installed.Generation); err != nil {
		recoveryErr := errors.Join(base.ErrRecoveryRequired, err)
		s.setFault(recoveryErr)
		return result, recoveryErr
	}
	if err := maintstate.RemoveWithFaultHook(s.core.root, s.maintenance.stateHook); err != nil {
		recoveryErr := errors.Join(base.ErrRecoveryRequired, err)
		s.setFault(recoveryErr)
		return result, recoveryErr
	}
	return result, nil
}

func (s *Store) validateRetirementProofs(inputs []recordlog.SegmentSummary, proofs []SegmentRetirementProof) (storecatalog.Manifest, error) {
	if len(inputs) == 0 || len(inputs) != len(proofs) {
		return storecatalog.Manifest{}, base.ErrInvalidConfig
	}
	published := s.PublishedState()
	if published == nil {
		return storecatalog.Manifest{}, base.ErrInvalidConfig
	}
	manifest := published.Manifest
	for index, proof := range proofs {
		if proof.Source != inputs[index] || proof.ManifestGeneration == 0 || proof.CatalogGeneration != proof.ManifestGeneration ||
			proof.ManifestGeneration != published.Generation || proof.CoveredCommitSeq != manifest.CoveredCommitSeq ||
			proof.ReplayStart != manifest.ReplayStart || !containsSealedSegment(manifest, proof.Source) {
			return storecatalog.Manifest{}, fmt.Errorf("retirement proof generation=%d published=%d: %w", proof.ManifestGeneration, published.Generation, base.ErrConflict)
		}
	}
	return manifest, nil
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

// openBatchCompactionOutputReferences prevents a long-lived unpublished root
// from being copied from one GC output into another on every scheduler pass.
// Normal sealed segments remain eligible for their first compaction; once the
// pending roots have moved into a dedicated output, that output waits for the
// owning Batch to commit, abort, or overwrite them.
func (s *Store) openBatchCompactionOutputReferences(sealed []recordlog.SegmentSummary) (map[recordlog.SegmentID]struct{}, error) {
	compactionOutputs := make(map[recordlog.SegmentID]struct{})
	for _, source := range sealed {
		if recordlog.IsCompactionSegment(source.SegmentID) {
			compactionOutputs[source.SegmentID] = struct{}{}
		}
	}
	if len(compactionOutputs) == 0 {
		return nil, nil
	}
	s.state.mu.Lock()
	if s.state.closed {
		s.state.mu.Unlock()
		return nil, base.ErrClosed
	}
	if s.state.fault != nil {
		s.state.mu.Unlock()
		return nil, errors.Join(base.ErrReadOnly, s.state.fault)
	}
	open := make([]*Batch, 0, len(s.state.open))
	for _, batch := range s.state.open {
		open = append(open, batch)
	}
	s.state.mu.Unlock()
	result := make(map[recordlog.SegmentID]struct{})
	for _, batch := range open {
		for _, ref := range batch.inner.PendingPutRefs() {
			segment := ref.Addr.SegmentID()
			if _, exists := compactionOutputs[segment]; exists {
				result[segment] = struct{}{}
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
	return s.proveSegmentRetirementAtCheckpoint(ctx, source, relocationEnd, true)
}

// proveCompactionRetirement consumes the exact post-relocation checkpoint.
// RecordRef live accounting proves that a known zero-live Segment has no
// Mapping references; the complete input scan already performed by this
// compaction need not be repeated. The open-Batch gate remains independent
// because unpublished Put refs are not part of Mapping stats.
func (s *Store) proveCompactionRetirement(source recordlog.SegmentID, relocationEnd model.CommitSeq) (SegmentRetirementProof, error) {
	return s.proveSegmentRetirementAtCheckpoint(context.Background(), source, relocationEnd, false)
}

func (s *Store) proveSegmentRetirementAtCheckpoint(ctx context.Context, source recordlog.SegmentID, relocationEnd model.CommitSeq, scanMapping bool) (SegmentRetirementProof, error) {
	s.mutationAdmission.writeLock()
	s.state.mu.Lock()
	closed, fault := s.state.closed, s.state.fault
	if closed {
		s.state.mu.Unlock()
		s.mutationAdmission.writeUnlock()
		return SegmentRetirementProof{}, base.ErrClosed
	}
	if fault != nil {
		s.state.mu.Unlock()
		s.mutationAdmission.writeUnlock()
		return SegmentRetirementProof{}, errors.Join(base.ErrReadOnly, fault)
	}
	open := make([]*Batch, 0, len(s.state.open))
	for _, batch := range s.state.open {
		open = append(open, batch)
	}
	s.state.mu.Unlock()
	s.mutationAdmission.writeUnlock()
	for _, batch := range open {
		if batch.inner.ReferencesSegment(source) {
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errors.New("open batch references source segment"))
		}
	}
	if commitFault := s.core.commits.Fault(); commitFault != nil {
		return SegmentRetirementProof{}, errors.Join(base.ErrReadOnly, commitFault)
	}

	manifest := s.catalogSnapshot()
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
	if scanMapping {
		err := s.core.compactionLog.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
			typ, err := recordcodec.TypeOf(payload)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if typ != recordcodec.RecordTypePut {
				return nil
			}
			put, err := recordcodec.DecodePut(payload, s.state.limits.MaxValueSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			current, exists, err := s.core.mapping.Lookup(put.RecordID)
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
	}
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == source && (stat.LiveBytes != 0 || stat.LiveRecords != 0) {
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errCheckpointStatsStale,
				fmt.Errorf("live_bytes=%d live_records=%d", stat.LiveBytes, stat.LiveRecords))
		}
	}
	return SegmentRetirementProof{
		Source: manifest.SealedDataSegments[index], CatalogGeneration: manifest.Generation, ManifestGeneration: manifest.Generation,
		CoveredCommitSeq: manifest.CoveredCommitSeq, ReplayStart: manifest.ReplayStart,
	}, nil
}

// relocateSegment requires the scheduler's data maintenance slot. Keeping orchestration ownership at
// Store lets a later complete GC operation compose relocation, checkpoint and
// retirement without recursively acquiring the maintenance lock.
func (s *Store) relocateSegment(ctx context.Context, source recordlog.SegmentID, rate uint64) (SegmentRelocationResult, error) {
	s.state.mu.Lock()
	closed, fault := s.state.closed, s.state.fault
	s.state.mu.Unlock()
	if closed {
		return SegmentRelocationResult{}, base.ErrClosed
	}
	if fault != nil {
		return SegmentRelocationResult{}, errors.Join(base.ErrReadOnly, fault)
	}
	if commitFault := s.core.commits.Fault(); commitFault != nil {
		return SegmentRelocationResult{}, errors.Join(base.ErrReadOnly, commitFault)
	}
	if s.core.catalog == nil || s.core.compactionLog == nil || s.maintenance.maxRelocationMutations == 0 {
		return SegmentRelocationResult{}, base.ErrInvalidConfig
	}
	manifest := s.catalogSnapshot()
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
	pending := make([]copiedRecord, 0, min(s.state.limits.MaxBatchMutations, s.maintenance.maxRelocationMutations))
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
		rawBatchID, err := s.core.batches.Allocate(ctx)
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
		published, err := s.relocateWithBudgetRetry(ctx, coordinator.Relocation{
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

	err = s.core.compactionLog.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
		result.ScannedRecords++
		typ, err := recordcodec.TypeOf(payload)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		if typ != recordcodec.RecordTypePut {
			return nil
		}
		result.PutRecords++
		put, err := recordcodec.DecodePut(payload, s.state.limits.MaxValueSize)
		if err != nil {
			return errors.Join(base.ErrCorrupt, err)
		}
		current, exists, err := s.core.mapping.Lookup(put.RecordID)
		if err != nil {
			return err
		}
		if !exists || current != scanned.Addr {
			return nil
		}
		result.LiveCandidates++
		valueBytes := uint64(len(put.Value))
		exceeds := func(limit uint64) bool { return valueBytes > limit || pendingBytes > limit-valueBytes }
		if len(pending) != 0 && (uint64(len(pending)) == s.state.limits.MaxBatchMutations ||
			uint64(len(pending)) == s.maintenance.maxRelocationMutations || exceeds(s.state.limits.MaxBatchBytes) ||
			exceeds(s.maintenance.maxRelocationBytes)) {
			if err := flush(); err != nil {
				return err
			}
		}
		copied, err := s.core.log.Append(ctx, payload, false)
		if err != nil {
			return err
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

// relocateWithBudgetRetry keeps transient Delta pressure out of the
// compaction failure path. ErrBudget is returned before queue admission, so
// retrying the same durable BatchID and proposal after a checkpoint is safe.
func (s *Store) relocateWithBudgetRetry(ctx context.Context, relocation coordinator.Relocation) (coordinator.RelocationResult, error) {
	for {
		result, err := s.core.commits.Relocate(ctx, relocation)
		if !errors.Is(err, mapping.ErrBudget) {
			if err == nil && result.DeltaPressureGeneration() != 0 {
				s.requestBackgroundCheckpoint(result.DeltaPressureGeneration())
			}
			return result, err
		}
		if err := s.awaitCheckpointPressure(ctx, result.DeltaPressureGeneration(), false); err != nil {
			return coordinator.RelocationResult{}, err
		}
	}
}

package engine

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

// SegmentRelocationResult describes only the copy-and-CAS phase of data GC.
// It does not imply checkpoint coverage or authorize Segment retirement.
type SegmentRelocationResult struct {
	ScannedRecords   uint64
	PutRecords       uint64
	LiveCandidates   uint64
	CopiedRecords    uint64
	CopiedValueBytes uint64
	Applied          uint64
	Skipped          uint64
	FirstCommitSeq   model.CommitSeq
	LastCommitSeq    model.CommitSeq
}

// SegmentRetirementProof captures the durable checkpoint at which source was
// proven to have no current Mapping or open-Batch references. It is not an
// authorization to unlink by itself: the eventual retire operation must
// consume and revalidate it while holding the maintenance/operation gates.
type SegmentRetirementProof struct {
	Source            recordlog.SegmentSummary
	CatalogGeneration uint64
	CoveredCommitSeq  model.CommitSeq
	ReplayStart       recordlog.LogPos
}

type copiedRecord struct {
	id      model.ID
	oldAddr recordlog.VAddr
	newAddr recordlog.VAddr
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

	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.relocateSegment(ctx, source)
}

// PrepareSegmentRetirement relocates current live records, checkpoints every
// resulting relocation, then proves under the exclusive operation gate that
// the source cannot be reintroduced by an already-open Batch.
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

	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	relocated, err := s.relocateSegment(ctx, source)
	if err != nil {
		return SegmentRetirementProof{}, relocated, err
	}
	if err := s.Checkpoint(ctx); err != nil {
		return SegmentRetirementProof{}, relocated, err
	}

	s.ops.Lock()
	defer s.ops.Unlock()
	proof, err := s.proveSegmentRetirementLocked(ctx, source, relocated.LastCommitSeq)
	return proof, relocated, err
}

func (s *Store) proveSegmentRetirementLocked(ctx context.Context, source recordlog.SegmentID, relocationEnd model.CommitSeq) (SegmentRetirementProof, error) {
	s.mu.Lock()
	closed, fault := s.closed, s.fault
	if closed {
		s.mu.Unlock()
		return SegmentRetirementProof{}, base.ErrClosed
	}
	if fault != nil {
		s.mu.Unlock()
		return SegmentRetirementProof{}, errors.Join(base.ErrReadOnly, fault)
	}
	for _, batch := range s.open {
		if batch.inner.ReferencesSegment(source) {
			s.mu.Unlock()
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errors.New("open batch references source segment"))
		}
	}
	s.mu.Unlock()
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
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == source && (stat.LiveBytes != 0 || stat.LiveRecords != 0) {
			return SegmentRetirementProof{}, errors.Join(base.ErrConflict, errors.New("checkpoint still maps source segment"))
		}
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
	return SegmentRetirementProof{
		Source: manifest.SealedDataSegments[index], CatalogGeneration: manifest.Generation,
		CoveredCommitSeq: manifest.CoveredCommitSeq, ReplayStart: manifest.ReplayStart,
	}, nil
}

// relocateSegment requires maintenanceMu. Keeping orchestration ownership at
// Store lets a later complete GC operation compose relocation, checkpoint and
// retirement without recursively acquiring the maintenance lock.
func (s *Store) relocateSegment(ctx context.Context, source recordlog.SegmentID) (SegmentRelocationResult, error) {
	s.ops.RLock()
	defer s.ops.RUnlock()

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

	var result SegmentRelocationResult
	pending := make([]copiedRecord, 0, min(s.limits.MaxBatchMutations, s.maxRelocationMutations))
	var pendingBytes uint64
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		rawBatchID, err := s.batches.Allocate(ctx)
		if err != nil {
			return err
		}
		changes := make([]mapping.Change, len(pending))
		for i, copied := range pending {
			changes[i] = mapping.Change{
				RecordID: copied.id, ExpectedOldAddr: copied.oldAddr,
				NewAddr: copied.newAddr, Operation: mapping.OperationRelocate,
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
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}

	err := s.maintenance.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
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
		if len(pending) != 0 && (uint64(len(pending)) == s.limits.MaxBatchMutations ||
			uint64(len(pending)) == s.maxRelocationMutations || pendingBytes > s.limits.MaxBatchBytes-valueBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		copied, err := s.log.Append(ctx, payload, false)
		if err != nil {
			return err
		}
		pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: scanned.Addr, newAddr: copied.Addr})
		pendingBytes += valueBytes
		result.CopiedRecords++
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
	return result, nil
}

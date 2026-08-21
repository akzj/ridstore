package ridstore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/segment"
)

type dataGCSession struct {
	store       *Store
	source      storeformat.FileSummary
	relocated   uint64
	skipped     uint64
	copiedBytes uint64
	lastCommit  base.CommitSeq
	cleaning    bool
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
		return &dataGCSession{store: s, source: candidate, cleaning: true}, nil
	}
	return nil, base.ErrNotFound
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
		rawBatchID, err := s.batchAllocator.Allocate(ctx)
		if err != nil {
			return err
		}
		batchID := base.BatchID(rawBatchID)
		s.mu.Lock()
		if rawBatchID >= s.issuedBatchHigh {
			s.issuedBatchHigh = rawBatchID + 1
		}
		s.mu.Unlock()
		result, err := s.coordinator.Relocate(ctx, batchID, changes)
		if err != nil {
			return err
		}
		g.relocated += uint64(result.Applied)
		g.skipped += uint64(result.Skipped)
		g.lastCommit = result.CommitSeq
		changes = changes[:0]
		batchBytes = 0
		return nil
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
	if err := s.segments.ScanCleaning(sourceID, func(addr base.VAddr, frame storeformat.Frame) error {
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
	}); err != nil {
		return err
	}
	return nil
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

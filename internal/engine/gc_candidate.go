package engine

import (
	"errors"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const compactionRatioScale = uint32(10_000)

// CompactionPolicy sets lower bounds for advisory candidate selection. Both
// enabled bounds must pass. Zero disables that bound.
type CompactionPolicy struct {
	MinReclaimableBytes      uint64
	MinReclaimableRatioBasis uint32
}

// SegmentCompactionCandidate is a checkpoint-derived scheduling hint. It does
// not authorize relocation or retirement and may become stale immediately.
type SegmentCompactionCandidate struct {
	Source                recordlog.SegmentSummary
	LiveBytesUpper        uint64
	LiveRecordsUpper      uint64
	ReclaimableBytesLower uint64
	ReclaimableRatioBasis uint32
	StatsCoveredCommitSeq model.CommitSeq
	CatalogGeneration     uint64
}

func selectCompactionCandidate(manifest storecatalog.Manifest, policy CompactionPolicy, excluded map[recordlog.SegmentID]struct{}) (SegmentCompactionCandidate, bool, error) {
	if policy.MinReclaimableRatioBasis > compactionRatioScale {
		return SegmentCompactionCandidate{}, false, base.ErrInvalidConfig
	}
	if manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq || !manifest.ReplayStart.Valid() {
		return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("invalid compaction checkpoint boundary"))
	}
	statIndex := 0
	var best SegmentCompactionCandidate
	found := false
	for _, source := range manifest.SealedDataSegments {
		for statIndex < len(manifest.SegmentStats) && manifest.SegmentStats[statIndex].SegmentID < source.SegmentID {
			return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("segment stats reference unknown source"))
		}
		var liveBytes, liveRecords uint64
		if statIndex < len(manifest.SegmentStats) && manifest.SegmentStats[statIndex].SegmentID == source.SegmentID {
			liveBytes = manifest.SegmentStats[statIndex].LiveBytes
			liveRecords = manifest.SegmentStats[statIndex].LiveRecords
			statIndex++
		}
		if _, blocked := excluded[source.SegmentID]; blocked {
			continue
		}
		if !storecatalog.StatsKnownForSegment(manifest.ReplayStart, source) {
			continue
		}
		if source.ValidEnd < recordlog.SegmentHeaderSize {
			return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("invalid sealed segment extent"))
		}
		physical := uint64(source.ValidEnd - recordlog.SegmentHeaderSize)
		if liveBytes > physical || liveRecords > source.RecordCount {
			return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("segment stats exceed source extent"))
		}
		reclaimable := physical - liveBytes
		if reclaimable == 0 || reclaimable < policy.MinReclaimableBytes {
			continue
		}
		ratio := uint32(reclaimable * uint64(compactionRatioScale) / physical)
		if ratio < policy.MinReclaimableRatioBasis {
			continue
		}
		candidate := SegmentCompactionCandidate{
			Source: source, LiveBytesUpper: liveBytes, LiveRecordsUpper: liveRecords,
			ReclaimableBytesLower: reclaimable, ReclaimableRatioBasis: ratio,
			StatsCoveredCommitSeq: manifest.StatsCoveredCommitSeq, CatalogGeneration: manifest.Generation,
		}
		if !found || betterCompactionCandidate(candidate, best) {
			best, found = candidate, true
		}
	}
	if statIndex != len(manifest.SegmentStats) {
		return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("segment stats reference unknown source"))
	}
	return best, found, nil
}

func betterCompactionCandidate(candidate, current SegmentCompactionCandidate) bool {
	if candidate.ReclaimableBytesLower != current.ReclaimableBytesLower {
		return candidate.ReclaimableBytesLower > current.ReclaimableBytesLower
	}
	if candidate.LiveBytesUpper != current.LiveBytesUpper {
		return candidate.LiveBytesUpper < current.LiveBytesUpper
	}
	return candidate.Source.SegmentID < current.Source.SegmentID
}

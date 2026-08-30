package engine

import (
	"errors"
	"time"

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
	MinStableRounds          uint32
	MaxDeathBytesPerCommit   uint64
	MaxDeathBytesPerSecond   uint64
	MinSegmentAge            time.Duration
	MaxInputSegments         uint32
	BypassCooldown           bool
}

// SegmentCompactionCandidate is a checkpoint-derived scheduling hint. It does
// not authorize relocation or retirement and may become stale immediately.
type SegmentCompactionCandidate struct {
	Source                recordlog.SegmentSummary
	Sources               []recordlog.SegmentSummary
	LiveBytesUpper        uint64
	LiveRecordsUpper      uint64
	ReclaimableBytesLower uint64
	ReclaimableRatioBasis uint32
	StatsCoveredCommitSeq model.CommitSeq
	CatalogGeneration     uint64
	StableRounds          uint32
	DeathBytesPerCommit   uint64
	DeathBytesPerSecond   uint64
}

func normalizeCompactionPolicy(policy CompactionPolicy) CompactionPolicy {
	if policy.MinStableRounds == 0 && !policy.BypassCooldown {
		policy.MinStableRounds = 2
	}
	if policy.MaxInputSegments == 0 {
		policy.MaxInputSegments = 4
	}
	return policy
}

func selectCompactionCandidate(manifest storecatalog.Manifest, policy CompactionPolicy, excluded map[recordlog.SegmentID]struct{}, stabilities ...func(recordlog.SegmentID) (gcStabilityView, bool)) (SegmentCompactionCandidate, bool, error) {
	policy = normalizeCompactionPolicy(policy)
	var stability func(recordlog.SegmentID) (gcStabilityView, bool)
	if len(stabilities) != 0 {
		stability = stabilities[0]
	}
	if policy.MinReclaimableRatioBasis > compactionRatioScale {
		return SegmentCompactionCandidate{}, false, base.ErrInvalidConfig
	}
	if manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq || !manifest.ReplayStart.Valid() {
		return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("invalid compaction checkpoint boundary"))
	}
	statIndex := 0
	eligible := make([]SegmentCompactionCandidate, 0, len(manifest.SealedDataSegments))
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
		view := gcStabilityView{}
		if !policy.BypassCooldown && stability != nil {
			var ok bool
			if stability != nil {
				view, ok = stability(source.SegmentID)
			}
			if !ok || view.StableRounds < policy.MinStableRounds || view.Age < policy.MinSegmentAge {
				continue
			}
		}
		candidate := SegmentCompactionCandidate{
			Source: source, Sources: []recordlog.SegmentSummary{source}, LiveBytesUpper: liveBytes, LiveRecordsUpper: liveRecords,
			ReclaimableBytesLower: reclaimable, ReclaimableRatioBasis: ratio,
			StatsCoveredCommitSeq: manifest.StatsCoveredCommitSeq, CatalogGeneration: manifest.Generation,
			StableRounds: view.StableRounds, DeathBytesPerCommit: view.LatestDeathPerCommit, DeathBytesPerSecond: view.LatestDeathBytesPerSec,
		}
		eligible = append(eligible, candidate)
	}
	if statIndex != len(manifest.SegmentStats) {
		return SegmentCompactionCandidate{}, false, errors.Join(base.ErrCorrupt, errors.New("segment stats reference unknown source"))
	}
	var best SegmentCompactionCandidate
	found := false
	for start := range eligible {
		group := eligible[start]
		physical := uint64(group.Source.ValidEnd - recordlog.SegmentHeaderSize)
		for end := start + 1; end < len(eligible) && uint32(len(group.Sources)) < policy.MaxInputSegments; end++ {
			next := eligible[end]
			if next.Source.SegmentID != group.Sources[len(group.Sources)-1].SegmentID+1 {
				break
			}
			group.Sources = append(group.Sources, next.Source)
			group.LiveBytesUpper += next.LiveBytesUpper
			group.LiveRecordsUpper += next.LiveRecordsUpper
			group.ReclaimableBytesLower += next.ReclaimableBytesLower
			physical += uint64(next.Source.ValidEnd - recordlog.SegmentHeaderSize)
			group.ReclaimableRatioBasis = uint32(group.ReclaimableBytesLower * uint64(compactionRatioScale) / physical)
			if next.StableRounds < group.StableRounds {
				group.StableRounds = next.StableRounds
			}
			group.DeathBytesPerCommit += next.DeathBytesPerCommit
			group.DeathBytesPerSecond += next.DeathBytesPerSecond
		}
		if !found || betterCompactionCandidate(group, best) {
			best, found = group, true
		}
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

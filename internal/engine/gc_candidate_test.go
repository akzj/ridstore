package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestSelectCompactionCandidateUsesSparseStatsAndStableOrdering(t *testing.T) {
	manifest := candidateManifest(t)
	manifest.SealedDataSegments = []recordlog.SegmentSummary{
		candidateSummary(t, 1, 320, 4),
		candidateSummary(t, 2, 320, 4),
		candidateSummary(t, 3, 320, 4),
	}
	manifest.SegmentStats = []storecatalog.SegmentStats{
		{SegmentID: 1, LiveBytes: 64, LiveRecords: 1},
		{SegmentID: 2, LiveBytes: 128, LiveRecords: 2},
	}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 4, Offset: 128}

	candidate, found, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil)
	if err != nil || !found {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
	if candidate.Source.SegmentID != 1 || len(candidate.Sources) != 3 || candidate.LiveBytesUpper != 192 || candidate.ReclaimableBytesLower != 576 || candidate.ReclaimableRatioBasis != 7_500 {
		t.Fatalf("candidate=%+v", candidate)
	}

	manifest.SegmentStats = append(manifest.SegmentStats, storecatalog.SegmentStats{SegmentID: 3, LiveBytes: 64, LiveRecords: 1})
	manifest.ActiveDataSegmentID = 4
	manifest.SegmentStats = append(manifest.SegmentStats, storecatalog.SegmentStats{SegmentID: 4, LiveBytes: 64, LiveRecords: 1})
	candidate, found, err = selectCompactionCandidate(manifest, CompactionPolicy{}, map[recordlog.SegmentID]struct{}{1: {}})
	if err != nil || !found || candidate.Source.SegmentID != 2 || len(candidate.Sources) != 2 {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
}

func TestSelectCompactionCandidateRequiresCooldownUnlessBypassed(t *testing.T) {
	manifest := candidateManifest(t)
	manifest.SealedDataSegments = []recordlog.SegmentSummary{candidateSummary(t, 1, 320, 4)}
	manifest.SegmentStats = []storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 64, LiveRecords: 1}}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 2, Offset: recordlog.SegmentHeaderSize}
	unstable := func(recordlog.SegmentID) (gcStabilityView, bool) {
		return gcStabilityView{Age: time.Hour, StableRounds: 1, LatestDeathPerCommit: 10}, true
	}
	if _, found, err := selectCompactionCandidate(manifest, CompactionPolicy{MinStableRounds: 2}, nil, unstable); err != nil || found {
		t.Fatalf("unstable candidate found=%v err=%v", found, err)
	}
	if candidate, found, err := selectCompactionCandidate(manifest, CompactionPolicy{MinStableRounds: 2, BypassCooldown: true}, nil, unstable); err != nil || !found || candidate.Source.SegmentID != 1 {
		t.Fatalf("bypass candidate=%+v found=%v err=%v", candidate, found, err)
	}
}

func TestSelectCompactionCandidateCapsAdjacentGroup(t *testing.T) {
	manifest := candidateManifest(t)
	for id := recordlog.SegmentID(1); id <= 4; id++ {
		manifest.SealedDataSegments = append(manifest.SealedDataSegments, candidateSummary(t, id, 320, 4))
	}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 5, Offset: recordlog.SegmentHeaderSize}
	candidate, found, err := selectCompactionCandidate(manifest, CompactionPolicy{MaxInputSegments: 2, BypassCooldown: true}, nil)
	if err != nil || !found || len(candidate.Sources) != 2 {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
}

func TestSelectCompactionCandidateHonorsReplayAndPolicy(t *testing.T) {
	manifest := candidateManifest(t)
	manifest.SealedDataSegments = []recordlog.SegmentSummary{
		candidateSummary(t, 1, 320, 4),
		candidateSummary(t, 2, 320, 4),
	}
	manifest.SegmentStats = []storecatalog.SegmentStats{
		{SegmentID: 1, LiveBytes: 64, LiveRecords: 1},
		{SegmentID: 2, LiveBytes: 128, LiveRecords: 2},
	}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 2, Offset: 192}

	candidate, found, err := selectCompactionCandidate(manifest, CompactionPolicy{
		MinReclaimableBytes: 192, MinReclaimableRatioBasis: 7_500,
	}, nil)
	if err != nil || !found || candidate.Source.SegmentID != 1 {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
	if _, found, err := selectCompactionCandidate(manifest, CompactionPolicy{MinReclaimableBytes: 193}, nil); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestSelectCompactionCandidateTreatsReplayBoundarySegmentAsUnknown(t *testing.T) {
	manifest := candidateManifest(t)
	segment := candidateSummary(t, 1, 320, 4)
	manifest.SealedDataSegments = []recordlog.SegmentSummary{segment}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 1, Offset: segment.ValidEnd}

	if _, found, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 2, Offset: recordlog.SegmentHeaderSize}
	if candidate, found, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil); err != nil || !found || candidate.Source.SegmentID != 1 {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
}

func TestSelectCompactionCandidateRejectsInvalidBoundsAndStats(t *testing.T) {
	manifest := candidateManifest(t)
	manifest.SealedDataSegments = []recordlog.SegmentSummary{candidateSummary(t, 1, 128, 1)}
	manifest.ReplayStart = recordlog.LogPos{SegmentID: 2, Offset: recordlog.SegmentHeaderSize}
	if _, _, err := selectCompactionCandidate(manifest, CompactionPolicy{MinReclaimableRatioBasis: 10_001}, nil); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("ratio err=%v", err)
	}
	manifest.StatsCoveredCommitSeq--
	if _, _, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("checkpoint boundary err=%v", err)
	}
	manifest.StatsCoveredCommitSeq++
	manifest.SegmentStats = []storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 65, LiveRecords: 1}}
	if _, _, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("stats err=%v", err)
	}
	manifest.SegmentStats = []storecatalog.SegmentStats{{SegmentID: 9, LiveBytes: 1, LiveRecords: 1}}
	if _, _, err := selectCompactionCandidate(manifest, CompactionPolicy{}, nil); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("unknown stats err=%v", err)
	}
}

func candidateManifest(t *testing.T) storecatalog.Manifest {
	t.Helper()
	return storecatalog.Manifest{
		Generation: 9, CoveredCommitSeq: model.CommitSeq(7), StatsCoveredCommitSeq: model.CommitSeq(7),
		ReplayStart: recordlog.LogPos{SegmentID: 1, Offset: recordlog.SegmentHeaderSize},
	}
}

func candidateSummary(t *testing.T, id recordlog.SegmentID, validEnd uint32, records uint64) recordlog.SegmentSummary {
	t.Helper()
	first, err := recordlog.NewVAddr(id, recordlog.SegmentHeaderSize, 64)
	if err != nil {
		t.Fatal(err)
	}
	last, err := recordlog.NewVAddr(id, validEnd-64, 64)
	if err != nil {
		t.Fatal(err)
	}
	return recordlog.SegmentSummary{SegmentID: id, ValidEnd: validEnd, RecordCount: records, FirstAddr: first, LastAddr: last}
}

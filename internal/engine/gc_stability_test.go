package engine

import (
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestGCStabilityWaitsForQuietRoundsAndResetsOnDeath(t *testing.T) {
	var history gcStability
	base := time.Unix(100, 0)
	manifest := storecatalog.Manifest{
		StatsCoveredCommitSeq: 10,
		SealedDataSegments:    []recordlog.SegmentSummary{{SegmentID: 1}},
		SegmentStats:          []storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 100, LiveRecords: 1}},
	}
	policy := CompactionPolicy{MinStableRounds: 2}
	history.sample(manifest, base)
	if view, _ := history.view(1, base, policy); view.StableRounds != 0 {
		t.Fatalf("view=%+v", view)
	}
	manifest.StatsCoveredCommitSeq = model.CommitSeq(11)
	history.sample(manifest, base.Add(time.Second))
	if view, _ := history.view(1, base.Add(time.Second), policy); view.StableRounds != 1 {
		t.Fatalf("view=%+v", view)
	}
	manifest.StatsCoveredCommitSeq = model.CommitSeq(12)
	history.sample(manifest, base.Add(2*time.Second))
	if view, _ := history.view(1, base.Add(2*time.Second), policy); view.StableRounds != 2 {
		t.Fatalf("view=%+v", view)
	}

	manifest.StatsCoveredCommitSeq = model.CommitSeq(13)
	manifest.SegmentStats[0].LiveBytes = 80
	history.sample(manifest, base.Add(3*time.Second))
	view, _ := history.view(1, base.Add(3*time.Second), policy)
	if view.StableRounds != 0 || view.LatestDeathBytes != 20 || view.LatestDeathPerCommit != 20 || view.LatestDeathBytesPerSec != 20 {
		t.Fatalf("view=%+v", view)
	}
}

func TestGCStabilityThresholdAndAge(t *testing.T) {
	var history gcStability
	base := time.Unix(100, 0)
	manifest := storecatalog.Manifest{StatsCoveredCommitSeq: 10, SealedDataSegments: []recordlog.SegmentSummary{{SegmentID: 1}}, SegmentStats: []storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 100, LiveRecords: 1}}}
	history.sample(manifest, base)
	manifest.StatsCoveredCommitSeq = 20
	manifest.SegmentStats[0].LiveBytes = 90
	history.sample(manifest, base.Add(2*time.Second))
	policy := CompactionPolicy{MaxDeathBytesPerCommit: 1, MaxDeathBytesPerSecond: 5, MinSegmentAge: time.Second}
	view, ok := history.view(1, base.Add(2*time.Second), policy)
	if !ok || view.StableRounds != 1 || view.Age != 2*time.Second {
		t.Fatalf("view=%+v ok=%v", view, ok)
	}
}

package verifier

import (
	"testing"

	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestEqualCoveredSegmentStatsAllowsMissingUnknownSegment(t *testing.T) {
	segment := storecatalog.DataSegmentSummary{SegmentID: 2, ValidEnd: 320}
	manifest := storecatalog.Manifest{
		ReplayStart:        recordlog.LogPos{SegmentID: 2, Offset: 192},
		SealedDataSegments: []storecatalog.DataSegmentSummary{segment},
	}
	exact := []storecatalog.SegmentStats{{SegmentID: 2, LiveBytes: 64, LiveRecords: 1}}
	if !equalCoveredSegmentStats(exact, nil, manifest) {
		t.Fatal("a Segment crossing ReplayStart may be absent from the checkpoint Stats table")
	}
}

func TestEqualCoveredSegmentStatsRejectsMissingKnownSegment(t *testing.T) {
	segment := storecatalog.DataSegmentSummary{SegmentID: 1, ValidEnd: 320}
	manifest := storecatalog.Manifest{
		ReplayStart:        recordlog.LogPos{SegmentID: 2, Offset: recordlog.SegmentHeaderSize},
		SealedDataSegments: []storecatalog.DataSegmentSummary{segment},
	}
	exact := []storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 64, LiveRecords: 1}}
	if equalCoveredSegmentStats(exact, nil, manifest) {
		t.Fatal("a Segment strictly behind ReplayStart must have complete checkpoint Stats")
	}
}

func TestEqualCoveredSegmentStatsAcceptsExplicitZero(t *testing.T) {
	segment := storecatalog.DataSegmentSummary{SegmentID: 1, ValidEnd: 320}
	manifest := storecatalog.Manifest{
		ReplayStart:        recordlog.LogPos{SegmentID: 2, Offset: recordlog.SegmentHeaderSize},
		SealedDataSegments: []storecatalog.DataSegmentSummary{segment},
	}
	recorded := []storecatalog.SegmentStats{{SegmentID: 1}}
	if !equalCoveredSegmentStats(nil, recorded, manifest) {
		t.Fatal("an explicit zero is equivalent to an omitted covered Stats entry")
	}
}

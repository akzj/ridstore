package ridstore

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/verifier"
)

// VerifyStage is the last completed offline verification stage.
type VerifyStage string

const (
	VerifyStageLocked    VerifyStage = "locked"
	VerifyStageManifest  VerifyStage = "manifest"
	VerifyStageRecordLog VerifyStage = "recordlog"
	VerifyStageMapping   VerifyStage = "mapping"
	VerifyStagePhysical  VerifyStage = "physical-complete"
	VerifyStageReachable VerifyStage = "mapping-reachable"
	VerifyStageSemantic  VerifyStage = "semantic-replay"
	VerifyStageExact     VerifyStage = "exact-join"
)

// VerifyConfig bounds the memory retained by offline verification. Zero
// fields receive defaults. Dir must name an initialized, closed v2 Store.
type VerifyConfig struct {
	Dir               string
	MappingCacheBytes uint64
	MaxLiveIDs        uint64
	MaxReplayStatuses uint64
}

// VerifyReport contains only facts proven through Stage. Exact is the only
// successful terminal stage.
type VerifyReport struct {
	Stage              VerifyStage
	ManifestGeneration uint64
	StoreID            [16]byte

	DataSegments       uint64
	SealedDataSegments uint64
	DataRecords        uint64
	DataPhysicalBytes  uint64
	ActiveDataSegment  uint32
	ActiveDataEnd      uint32

	MappingSegments       uint64
	SealedMappingSegments uint64
	MappingNodes          uint64
	MappingPhysicalBytes  uint64
	ActiveMappingEnd      uint32

	CheckpointLiveIDs uint64
	LiveIDs           uint64
	ReplayedCommits   uint64
	BatchStatuses     uint64
	NextCommitSeq     CommitSeq
	VerifiedPuts      uint64
	VerifiedStats     uint64
}

// Verify performs a strictly read-only offline audit. It requires the Store's
// exclusive directory lock and never invokes Open, recovery, or repair.
func Verify(ctx context.Context, config VerifyConfig) (VerifyReport, error) {
	config = config.withDefaults()
	report, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: config.MappingCacheBytes,
		MaxLiveIDs:        config.MaxLiveIDs,
		MaxReplayStatuses: config.MaxReplayStatuses,
	})
	public := VerifyReport{
		Stage: VerifyStage(report.Stage), ManifestGeneration: report.ManifestGeneration, StoreID: report.StoreID,
		DataSegments: report.Data.Segments, SealedDataSegments: report.Data.SealedSegments,
		DataRecords: report.Data.Records, DataPhysicalBytes: report.Data.PhysicalBytes,
		ActiveDataSegment: uint32(report.Data.ActiveEnd.SegmentID), ActiveDataEnd: report.Data.ActiveEnd.Offset,
		MappingSegments: report.Mapping.Segments, SealedMappingSegments: report.Mapping.SealedSegments,
		MappingNodes: report.Mapping.Nodes, MappingPhysicalBytes: report.Mapping.PhysicalBytes,
		ActiveMappingEnd:  report.Mapping.ActiveEnd,
		CheckpointLiveIDs: report.CheckpointLiveIDs, LiveIDs: report.LiveIDs,
		ReplayedCommits: report.ReplayedCommits, BatchStatuses: report.BatchStatuses,
		NextCommitSeq: report.NextCommitSeq, VerifiedPuts: report.VerifiedPuts, VerifiedStats: report.VerifiedStats,
	}
	if errors.Is(err, verifier.ErrLimit) {
		return public, errors.Join(ErrVerifyLimit, err)
	}
	return public, err
}

func (c VerifyConfig) withDefaults() VerifyConfig {
	if c.MappingCacheBytes == 0 {
		c.MappingCacheBytes = 256 * mib
	}
	if c.MaxLiveIDs == 0 {
		c.MaxLiveIDs = 1 << 20
	}
	if c.MaxReplayStatuses == 0 {
		c.MaxReplayStatuses = 1 << 20
	}
	return c
}

package ridstore

import (
	"time"

	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/engine"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const mib = uint64(1 << 20)

// HardLimits are persisted at creation and define the on-disk format bounds.
// Zero fields receive defaults.
type HardLimits struct {
	SegmentSize         uint64
	MaxValueSize        uint64
	MaxBatchBytes       uint64
	MaxBatchMutations   uint64
	MaxBatchConditions  uint64
	MaxOpenBatches      uint64
	MaxRecordLogPayload uint64
	IDReserveSize       uint64
	BatchIDReserveSize  uint64
}

// RuntimeConfig contains replaceable memory, queue, and batching budgets.
// Zero fields receive defaults on every Create or Open.
type RuntimeConfig struct {
	MaxQueuedBytes      uint64
	AppendQueueCapacity int
	AppendBufferBytes   uint32
	AppendBufferRecords int
	CommitQueueCapacity int
	MaxGroupBatches     int
	MaxGroupPayload     uint64
	// GroupCommitDelay bounds commit coalescing. Zero selects the default;
	// DisableGroupCommitDelay retains opportunistic, non-blocking batching.
	GroupCommitDelay        time.Duration
	DisableGroupCommitDelay bool
	MappingCacheBytes       uint64
	CheckpointSortBytes     uint64
	MaxSegmentStats         uint64
	DeltaSoftLimitBytes     uint64
	DeltaHardLimitBytes     uint64
	// StatusRetention bounds retained/replayed user Batch outcomes and must be
	// at least HardLimits.MaxOpenBatches.
	StatusRetention uint64
	// WriteStopFreeBytes reserves filesystem headroom for commits, checkpoints,
	// and GC after new user Put records have been stopped.
	WriteStopFreeBytes uint64
	SpaceCheckInterval time.Duration
	// CheckpointInterval bounds how long a non-empty Mapping Delta remains
	// uncheckpointed when it stays below the pressure threshold.
	CheckpointInterval time.Duration
	// GCBatchBytes bounds copied Value bytes and GCBatchMutations bounds changes
	// in one relocation publication. A single legal value may exceed the
	// runtime byte budget and is relocated alone.
	GCBatchBytes     uint64
	GCBatchMutations uint64
	// GCMinFreeBytes is the filesystem headroom retained while GC copies data
	// and builds its required checkpoint. GCBytesPerSecond bounds copy rate.
	GCMinFreeBytes   uint64
	GCBytesPerSecond uint64
}

type CreateConfig struct {
	Dir        string
	HardLimits HardLimits
	Runtime    RuntimeConfig
}

type OpenConfig struct {
	Dir     string
	Runtime RuntimeConfig
}

func (c CreateConfig) engineConfig() engine.CreateConfig {
	hard := c.HardLimits.withDefaults()
	return engine.CreateConfig{HardLimits: storecatalog.HardLimits{
		SegmentSize: hard.SegmentSize, MaxValueSize: hard.MaxValueSize,
		MaxBatchBytes: hard.MaxBatchBytes, MaxBatchMutations: hard.MaxBatchMutations,
		MaxBatchConditions: hard.MaxBatchConditions, MaxOpenBatches: hard.MaxOpenBatches,
		MaxRecordLogPayload: hard.MaxRecordLogPayload, IDReserveSize: hard.IDReserveSize,
		BatchIDReserveSize: hard.BatchIDReserveSize,
	}, Runtime: c.Runtime.engineConfig()}
}

func (c OpenConfig) engineConfig() engine.OpenConfig { return c.Runtime.engineConfig() }

func (h HardLimits) withDefaults() HardLimits {
	if h.SegmentSize == 0 {
		h.SegmentSize = 256 * mib
	}
	if h.MaxValueSize == 0 {
		h.MaxValueSize = 64 * mib
	}
	if h.MaxBatchBytes == 0 {
		h.MaxBatchBytes = 256 * mib
	}
	if h.MaxBatchMutations == 0 {
		h.MaxBatchMutations = 1_000_000
	}
	if h.MaxBatchConditions == 0 {
		h.MaxBatchConditions = 1_000_000
	}
	if h.MaxOpenBatches == 0 {
		h.MaxOpenBatches = 1024
	}
	if h.MaxRecordLogPayload == 0 {
		h.MaxRecordLogPayload = 128 * mib
	}
	if h.IDReserveSize == 0 {
		h.IDReserveSize = 1 << 20
	}
	if h.BatchIDReserveSize == 0 {
		h.BatchIDReserveSize = 1 << 16
	}
	return h
}

func (c RuntimeConfig) engineConfig() engine.OpenConfig {
	if c.MaxQueuedBytes == 0 {
		c.MaxQueuedBytes = 256 * mib
	}
	if c.AppendQueueCapacity == 0 {
		c.AppendQueueCapacity = 1024
	}
	if c.AppendBufferBytes == 0 {
		c.AppendBufferBytes = 8 << 20
	}
	if c.AppendBufferRecords == 0 {
		c.AppendBufferRecords = 4096
	}
	if c.CommitQueueCapacity == 0 {
		c.CommitQueueCapacity = 1024
	}
	if c.MaxGroupBatches == 0 {
		c.MaxGroupBatches = 64
	}
	if c.MaxGroupPayload == 0 {
		c.MaxGroupPayload = 64 * mib
	}
	if c.DisableGroupCommitDelay {
		c.GroupCommitDelay = 0
	} else if c.GroupCommitDelay == 0 {
		c.GroupCommitDelay = 50 * time.Microsecond
	}
	if c.MappingCacheBytes == 0 {
		c.MappingCacheBytes = 256 * mib
	}
	if c.CheckpointSortBytes == 0 {
		c.CheckpointSortBytes = 256 * mib
	}
	if c.MaxSegmentStats == 0 {
		c.MaxSegmentStats = 1 << 16
	}
	if c.DeltaSoftLimitBytes == 0 {
		c.DeltaSoftLimitBytes = 256 * mib
	}
	if c.DeltaHardLimitBytes == 0 {
		c.DeltaHardLimitBytes = 512 * mib
	}
	if c.StatusRetention == 0 {
		c.StatusRetention = 1 << 16
	}
	if c.WriteStopFreeBytes == 0 {
		c.WriteStopFreeBytes = 512 * mib
	}
	if c.SpaceCheckInterval == 0 {
		c.SpaceCheckInterval = 100 * time.Millisecond
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 30 * time.Second
	}
	return engine.OpenConfig{
		RecordLog:         recordlog.Config{MaxQueuedBytes: c.MaxQueuedBytes, QueueCapacity: c.AppendQueueCapacity, BufferBytes: c.AppendBufferBytes, BufferRecords: c.AppendBufferRecords},
		Commit:            coordinator.Config{QueueCapacity: c.CommitQueueCapacity, MaxGroupBatches: c.MaxGroupBatches, MaxGroupPayload: c.MaxGroupPayload, GroupCommitDelay: c.GroupCommitDelay},
		MappingCacheBytes: c.MappingCacheBytes, CheckpointSortBytes: c.CheckpointSortBytes,
		MaxSegmentStats: c.MaxSegmentStats, DeltaSoftLimitBytes: c.DeltaSoftLimitBytes,
		DeltaHardLimitBytes: c.DeltaHardLimitBytes, StatusRetention: c.StatusRetention,
		WriteStopFreeBytes: c.WriteStopFreeBytes, SpaceCheckInterval: c.SpaceCheckInterval,
		CheckpointInterval: c.CheckpointInterval,
		GCBatchBytes:       c.GCBatchBytes, GCBatchMutations: c.GCBatchMutations,
		GCMinFreeBytes: c.GCMinFreeBytes, GCBytesPerSecond: c.GCBytesPerSecond,
	}
}

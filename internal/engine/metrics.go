package engine

import "sync/atomic"

type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches             uint64
	Committed, Aborted, Conflicts, CommitUnknown         uint64
	QueueWaitNanos, ValidationNanos                      uint64
	WriteSyncNanos, PublishNanos                         uint64
	DeltaChargedBytes, DeltaReservedBytes                uint64
	DeltaSoftLimitBytes, DeltaHardLimitBytes             uint64
	MappingCacheBytes                                    uint64
	DiskAvailableEstimateBytes, WriteStopFreeBytes       uint64
	WriteStopped                                         uint64
	WriteStopRejections, DiskSpaceCheckErrors            uint64
	GCStarted, GCCompleted, GCFailed                     uint64
	GCNoCandidate                                        uint64
	GCCopiedBytes, GCReclaimedBytes                      uint64
	GCRelocated, GCSkipped, GCDurationNanos              uint64
	GCThrottledNanos, GCSpaceRejections                  uint64
	GCCommitRedirects, GCCommitRedirectWaitNanos         uint64
	GCCommitRedirectAdmissionNanos, GCOpenRefsRedirected uint64
	GCMinFreeBytes, GCBytesPerSecond                     uint64
	BackgroundCheckpointRequested                        uint64
	BackgroundCheckpointCompleted                        uint64
	BackgroundCheckpointFailed                           uint64
}

type runtimeMetrics struct {
	committed, aborted, unknown                          atomic.Uint64
	gcStarted, gcCompleted, gcFailed                     atomic.Uint64
	gcNoCandidate                                        atomic.Uint64
	gcCopiedBytes, gcReclaimedBytes                      atomic.Uint64
	gcRelocated, gcSkipped, gcDurationNanos              atomic.Uint64
	gcThrottledNanos, gcSpaceRejections                  atomic.Uint64
	gcCommitRedirects, gcCommitRedirectWaitNanos         atomic.Uint64
	gcCommitRedirectAdmissionNanos, gcOpenRefsRedirected atomic.Uint64
	backgroundCheckpointRequested                        atomic.Uint64
	backgroundCheckpointCompleted                        atomic.Uint64
	backgroundCheckpointFailed                           atomic.Uint64
}

func (s *Store) Metrics() Metrics {
	if s == nil {
		return Metrics{}
	}
	commit := s.commits.Metrics()
	result := Metrics{
		CommitQueued: commit.CommitQueued, CommitGroups: commit.CommitGroups, GroupBatches: commit.GroupBatches,
		Committed: s.metrics.committed.Load(), Aborted: s.metrics.aborted.Load(), Conflicts: commit.Conflicts,
		CommitUnknown: s.metrics.unknown.Load(), QueueWaitNanos: commit.QueueWaitNanos,
		ValidationNanos: commit.ValidationNanos, WriteSyncNanos: commit.WriteSyncNanos, PublishNanos: commit.PublishNanos,
		GCStarted: s.metrics.gcStarted.Load(), GCCompleted: s.metrics.gcCompleted.Load(), GCFailed: s.metrics.gcFailed.Load(),
		GCNoCandidate: s.metrics.gcNoCandidate.Load(),
		GCCopiedBytes: s.metrics.gcCopiedBytes.Load(), GCReclaimedBytes: s.metrics.gcReclaimedBytes.Load(),
		GCRelocated: s.metrics.gcRelocated.Load(), GCSkipped: s.metrics.gcSkipped.Load(),
		GCDurationNanos: s.metrics.gcDurationNanos.Load(), GCThrottledNanos: s.metrics.gcThrottledNanos.Load(),
		GCSpaceRejections: s.metrics.gcSpaceRejections.Load(),
		GCCommitRedirects: s.metrics.gcCommitRedirects.Load(), GCCommitRedirectWaitNanos: s.metrics.gcCommitRedirectWaitNanos.Load(),
		GCCommitRedirectAdmissionNanos: s.metrics.gcCommitRedirectAdmissionNanos.Load(), GCOpenRefsRedirected: s.metrics.gcOpenRefsRedirected.Load(),
		GCMinFreeBytes:                s.gcMinFreeBytes,
		GCBytesPerSecond:              s.gcBytesPerSecond.Load(),
		BackgroundCheckpointRequested: s.metrics.backgroundCheckpointRequested.Load(),
		BackgroundCheckpointCompleted: s.metrics.backgroundCheckpointCompleted.Load(),
		BackgroundCheckpointFailed:    s.metrics.backgroundCheckpointFailed.Load(),
	}
	if s.mapping != nil {
		result.DeltaChargedBytes, result.DeltaReservedBytes, result.DeltaSoftLimitBytes, result.DeltaHardLimitBytes = s.mapping.DeltaUsage()
		result.MappingCacheBytes = s.mapping.CacheBytes()
	}
	if s.space != nil {
		space := s.space.snapshot()
		result.DiskAvailableEstimateBytes = space.available
		result.WriteStopFreeBytes = space.minimum
		result.WriteStopRejections = space.rejections
		result.DiskSpaceCheckErrors = space.checkErrors
		if space.stopped {
			result.WriteStopped = 1
		}
	}
	return result
}

package engine

import "sync/atomic"

type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches                                            uint64
	Committed, Aborted, Conflicts, CommitUnknown                                        uint64
	QueueWaitNanos, ValidationNanos                                                     uint64
	WriteSyncNanos, PublishNanos                                                        uint64
	CheckpointFences, CheckpointFenceAcquireNanos                                       uint64
	CheckpointFenceHeldNanos, CheckpointFenceMaxHeldNanos                               uint64
	CheckpointsStarted, CheckpointsCompleted, CheckpointsFailed                         uint64
	CheckpointDurationNanos, CheckpointMaxDurationNanos                                 uint64
	CheckpointCaptureWaitNanos, CheckpointMaxCaptureWaitNanos                           uint64
	CheckpointCaptureNanos, CheckpointMaxCaptureNanos                                   uint64
	CheckpointBuildNanos, CheckpointMaxBuildNanos                                       uint64
	CheckpointPublishNanos, CheckpointMaxPublishNanos                                   uint64
	CheckpointCaptureConflicts, CheckpointPublishConflicts                              uint64
	RecordLogRotations, RecordLogRotationNanos, RecordLogRotationMaxNanos               uint64
	MappingGCStarted, MappingGCCompleted, MappingGCFailed, MappingGCConflicts           uint64
	MappingGCDurationNanos, MappingGCMaxDurationNanos                                   uint64
	MappingGCRebuildNanos, MappingGCVerifyNanos                                         uint64
	MappingGCPublishNanos, MappingGCMaxPublishNanos                                     uint64
	DeltaChargedBytes, DeltaReservedBytes                                               uint64
	DeltaSoftLimitBytes, DeltaHardLimitBytes                                            uint64
	MappingCacheBytes                                                                   uint64
	DiskAvailableEstimateBytes, WriteStopFreeBytes                                      uint64
	WriteStopped                                                                        uint64
	WriteStopRejections, DiskSpaceCheckErrors                                           uint64
	GCStarted, GCCompleted, GCFailed                                                    uint64
	GCNoCandidate                                                                       uint64
	GCCopiedBytes, GCReclaimedBytes                                                     uint64
	GCRelocated, GCSkipped, GCDurationNanos                                             uint64
	GCThrottledNanos, GCSpaceRejections                                                 uint64
	GCCommitRedirects, GCCommitRedirectWaitNanos                                        uint64
	GCCommitRedirectAdmissionNanos, GCOpenRefsRedirected                                uint64
	GCMinFreeBytes, GCBytesPerSecond                                                    uint64
	BackgroundCheckpointRequested                                                       uint64
	BackgroundCheckpointCompleted                                                       uint64
	BackgroundCheckpointFailed                                                          uint64
	MappingSurveyGeneration, MappingSurveyPhysicalBytes, MappingSurveyReachableBytes    uint64
	MaintenanceAutomaticFailed                                                          uint64
	MaintenanceRequested, MaintenanceCoalesced, MaintenanceCompleted, MaintenanceFailed uint64
	MaintenancePreemptions, MaintenanceQueued, MaintenanceRunning                       uint64
	MaintenanceQueueWaitNanos, MaintenanceMaxQueueWaitNanos                             uint64
	MaintenanceRunNanos, MaintenanceMaxRunNanos                                         uint64
	MaintenanceRetries, MaintenanceInvariantViolations                                  uint64
}

type runtimeMetrics struct {
	committed, aborted, unknown                                                      atomic.Uint64
	gcStarted, gcCompleted, gcFailed                                                 atomic.Uint64
	gcNoCandidate                                                                    atomic.Uint64
	gcCopiedBytes, gcReclaimedBytes                                                  atomic.Uint64
	gcRelocated, gcSkipped, gcDurationNanos                                          atomic.Uint64
	gcThrottledNanos, gcSpaceRejections                                              atomic.Uint64
	gcCommitRedirects, gcCommitRedirectWaitNanos                                     atomic.Uint64
	gcCommitRedirectAdmissionNanos, gcOpenRefsRedirected                             atomic.Uint64
	backgroundCheckpointRequested                                                    atomic.Uint64
	backgroundCheckpointCompleted                                                    atomic.Uint64
	backgroundCheckpointFailed                                                       atomic.Uint64
	mappingSurveyGeneration, mappingSurveyPhysicalBytes, mappingSurveyReachableBytes atomic.Uint64
	maintenanceAutomaticFailed                                                       atomic.Uint64
	checkpointsStarted, checkpointsCompleted, checkpointsFailed                      atomic.Uint64
	checkpointDurationNanos, checkpointMaxDurationNanos                              atomic.Uint64
	checkpointCaptureWaitNanos, checkpointMaxCaptureWaitNanos                        atomic.Uint64
	checkpointCaptureNanos, checkpointMaxCaptureNanos                                atomic.Uint64
	checkpointBuildNanos, checkpointMaxBuildNanos                                    atomic.Uint64
	checkpointPublishNanos, checkpointMaxPublishNanos                                atomic.Uint64
	checkpointCaptureConflicts, checkpointPublishConflicts                           atomic.Uint64
	mappingGCStarted, mappingGCCompleted, mappingGCFailed, mappingGCConflicts        atomic.Uint64
	mappingGCDurationNanos, mappingGCMaxDurationNanos                                atomic.Uint64
	mappingGCRebuildNanos, mappingGCVerifyNanos                                      atomic.Uint64
	mappingGCPublishNanos, mappingGCMaxPublishNanos                                  atomic.Uint64
}

func (s *Store) Metrics() Metrics {
	if s == nil {
		return Metrics{}
	}
	commit := s.core.commits.Metrics()
	result := Metrics{
		CommitQueued: commit.CommitQueued, CommitGroups: commit.CommitGroups, GroupBatches: commit.GroupBatches,
		Committed: s.metrics.committed.Load(), Aborted: s.metrics.aborted.Load(), Conflicts: commit.Conflicts,
		CommitUnknown: s.metrics.unknown.Load(), QueueWaitNanos: commit.QueueWaitNanos,
		ValidationNanos: commit.ValidationNanos, WriteSyncNanos: commit.WriteSyncNanos, PublishNanos: commit.PublishNanos,
		CheckpointFences: commit.CheckpointFences, CheckpointFenceAcquireNanos: commit.CheckpointFenceAcquireNanos,
		CheckpointFenceHeldNanos: commit.CheckpointFenceHeldNanos, CheckpointFenceMaxHeldNanos: commit.CheckpointFenceMaxHeldNanos,
		CheckpointsStarted: s.metrics.checkpointsStarted.Load(), CheckpointsCompleted: s.metrics.checkpointsCompleted.Load(),
		CheckpointsFailed: s.metrics.checkpointsFailed.Load(), CheckpointDurationNanos: s.metrics.checkpointDurationNanos.Load(),
		CheckpointMaxDurationNanos: s.metrics.checkpointMaxDurationNanos.Load(),
		CheckpointCaptureWaitNanos: s.metrics.checkpointCaptureWaitNanos.Load(), CheckpointMaxCaptureWaitNanos: s.metrics.checkpointMaxCaptureWaitNanos.Load(),
		CheckpointCaptureNanos: s.metrics.checkpointCaptureNanos.Load(), CheckpointMaxCaptureNanos: s.metrics.checkpointMaxCaptureNanos.Load(),
		CheckpointBuildNanos: s.metrics.checkpointBuildNanos.Load(), CheckpointMaxBuildNanos: s.metrics.checkpointMaxBuildNanos.Load(),
		CheckpointPublishNanos: s.metrics.checkpointPublishNanos.Load(), CheckpointMaxPublishNanos: s.metrics.checkpointMaxPublishNanos.Load(),
		CheckpointCaptureConflicts: s.metrics.checkpointCaptureConflicts.Load(), CheckpointPublishConflicts: s.metrics.checkpointPublishConflicts.Load(),
		MappingGCStarted: s.metrics.mappingGCStarted.Load(), MappingGCCompleted: s.metrics.mappingGCCompleted.Load(),
		MappingGCFailed: s.metrics.mappingGCFailed.Load(), MappingGCConflicts: s.metrics.mappingGCConflicts.Load(),
		MappingGCDurationNanos:    s.metrics.mappingGCDurationNanos.Load(),
		MappingGCMaxDurationNanos: s.metrics.mappingGCMaxDurationNanos.Load(), MappingGCRebuildNanos: s.metrics.mappingGCRebuildNanos.Load(),
		MappingGCVerifyNanos: s.metrics.mappingGCVerifyNanos.Load(), MappingGCPublishNanos: s.metrics.mappingGCPublishNanos.Load(),
		MappingGCMaxPublishNanos: s.metrics.mappingGCMaxPublishNanos.Load(),
		GCStarted:                s.metrics.gcStarted.Load(), GCCompleted: s.metrics.gcCompleted.Load(), GCFailed: s.metrics.gcFailed.Load(),
		GCNoCandidate: s.metrics.gcNoCandidate.Load(),
		GCCopiedBytes: s.metrics.gcCopiedBytes.Load(), GCReclaimedBytes: s.metrics.gcReclaimedBytes.Load(),
		GCRelocated: s.metrics.gcRelocated.Load(), GCSkipped: s.metrics.gcSkipped.Load(),
		GCDurationNanos: s.metrics.gcDurationNanos.Load(), GCThrottledNanos: s.metrics.gcThrottledNanos.Load(),
		GCSpaceRejections: s.metrics.gcSpaceRejections.Load(),
		GCCommitRedirects: s.metrics.gcCommitRedirects.Load(), GCCommitRedirectWaitNanos: s.metrics.gcCommitRedirectWaitNanos.Load(),
		GCCommitRedirectAdmissionNanos: s.metrics.gcCommitRedirectAdmissionNanos.Load(), GCOpenRefsRedirected: s.metrics.gcOpenRefsRedirected.Load(),
		GCMinFreeBytes:                s.maintenance.gcMinFreeBytes,
		GCBytesPerSecond:              s.maintenance.gcBytesPerSecond.Load(),
		BackgroundCheckpointRequested: s.metrics.backgroundCheckpointRequested.Load(),
		BackgroundCheckpointCompleted: s.metrics.backgroundCheckpointCompleted.Load(),
		BackgroundCheckpointFailed:    s.metrics.backgroundCheckpointFailed.Load(),
		MappingSurveyGeneration:       s.metrics.mappingSurveyGeneration.Load(),
		MappingSurveyPhysicalBytes:    s.metrics.mappingSurveyPhysicalBytes.Load(),
		MappingSurveyReachableBytes:   s.metrics.mappingSurveyReachableBytes.Load(),
		MaintenanceAutomaticFailed:    s.metrics.maintenanceAutomaticFailed.Load(),
	}
	if s.core.log != nil {
		status := s.core.log.Status()
		result.RecordLogRotations = status.RotationCalls
		result.RecordLogRotationNanos = status.RotationNanos
		result.RecordLogRotationMaxNanos = status.RotationMaxNanos
	}
	scheduler := s.maintenance.scheduler.metrics()
	result.MaintenanceRequested, result.MaintenanceCoalesced = scheduler.requested, scheduler.coalesced
	result.MaintenanceCompleted, result.MaintenanceFailed = scheduler.completed, scheduler.failed
	result.MaintenancePreemptions, result.MaintenanceQueued, result.MaintenanceRunning = scheduler.preemptions, scheduler.queued, scheduler.running
	result.MaintenanceQueueWaitNanos, result.MaintenanceMaxQueueWaitNanos = scheduler.queueWaitNanos, scheduler.maxQueueWaitNanos
	result.MaintenanceRunNanos, result.MaintenanceMaxRunNanos = scheduler.runNanos, scheduler.maxRunNanos
	result.MaintenanceRetries, result.MaintenanceInvariantViolations = scheduler.retries, scheduler.invariantViolations
	if s.core.mapping != nil {
		result.DeltaChargedBytes, result.DeltaReservedBytes, result.DeltaSoftLimitBytes, result.DeltaHardLimitBytes = s.core.mapping.DeltaUsage()
		result.MappingCacheBytes = s.core.mapping.CacheBytes()
	}
	if s.core.space != nil {
		space := s.core.space.snapshot()
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

func updateAtomicMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

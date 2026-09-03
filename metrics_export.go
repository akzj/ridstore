package ridstore

// Metrics is a bounded, non-transactional runtime snapshot. Counters cover the
// current process lifetime; gauges describe the instant at which they were
// sampled. Metrics never authorize recovery, checkpoint, or GC decisions.
type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches                                            uint64
	Committed, Aborted, Conflicts, CommitUnknown                                        uint64
	QueueWaitNanos, ValidationNanos                                                     uint64
	WriteSyncNanos, PublishNanos                                                        uint64
	CheckpointFences, CheckpointFenceAcquireNanos                                       uint64
	CheckpointFenceHeldNanos, CheckpointFenceMaxHeldNanos                               uint64
	CheckpointsStarted, CheckpointsCompleted, CheckpointsFailed                         uint64
	CheckpointDurationNanos, CheckpointMaxDurationNanos                                 uint64
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
}

type MetricKind uint8

const (
	MetricCounter MetricKind = iota + 1
	MetricGauge
)

const MetricSampleCount = 74

type MetricSample struct {
	Name  string
	Kind  MetricKind
	Value uint64
}

func (m Metrics) AppendMetricSamples(dst []MetricSample) []MetricSample {
	return append(dst,
		MetricSample{"ridstore_commit_queued_total", MetricCounter, m.CommitQueued},
		MetricSample{"ridstore_commit_groups_total", MetricCounter, m.CommitGroups},
		MetricSample{"ridstore_commit_group_batches_total", MetricCounter, m.GroupBatches},
		MetricSample{"ridstore_committed_total", MetricCounter, m.Committed},
		MetricSample{"ridstore_aborted_total", MetricCounter, m.Aborted},
		MetricSample{"ridstore_conflicts_total", MetricCounter, m.Conflicts},
		MetricSample{"ridstore_commit_unknown_total", MetricCounter, m.CommitUnknown},
		MetricSample{"ridstore_queue_wait_nanoseconds_total", MetricCounter, m.QueueWaitNanos},
		MetricSample{"ridstore_validation_nanoseconds_total", MetricCounter, m.ValidationNanos},
		MetricSample{"ridstore_write_sync_nanoseconds_total", MetricCounter, m.WriteSyncNanos},
		MetricSample{"ridstore_publish_nanoseconds_total", MetricCounter, m.PublishNanos},
		MetricSample{"ridstore_checkpoint_fences_total", MetricCounter, m.CheckpointFences},
		MetricSample{"ridstore_checkpoint_fence_acquire_nanoseconds_total", MetricCounter, m.CheckpointFenceAcquireNanos},
		MetricSample{"ridstore_checkpoint_fence_held_nanoseconds_total", MetricCounter, m.CheckpointFenceHeldNanos},
		MetricSample{"ridstore_checkpoint_fence_max_held_nanoseconds", MetricGauge, m.CheckpointFenceMaxHeldNanos},
		MetricSample{"ridstore_checkpoints_started_total", MetricCounter, m.CheckpointsStarted},
		MetricSample{"ridstore_checkpoints_completed_total", MetricCounter, m.CheckpointsCompleted},
		MetricSample{"ridstore_checkpoints_failed_total", MetricCounter, m.CheckpointsFailed},
		MetricSample{"ridstore_checkpoint_duration_nanoseconds_total", MetricCounter, m.CheckpointDurationNanos},
		MetricSample{"ridstore_checkpoint_max_duration_nanoseconds", MetricGauge, m.CheckpointMaxDurationNanos},
		MetricSample{"ridstore_record_log_rotations_total", MetricCounter, m.RecordLogRotations},
		MetricSample{"ridstore_record_log_rotation_nanoseconds_total", MetricCounter, m.RecordLogRotationNanos},
		MetricSample{"ridstore_record_log_rotation_max_nanoseconds", MetricGauge, m.RecordLogRotationMaxNanos},
		MetricSample{"ridstore_mapping_gc_started_total", MetricCounter, m.MappingGCStarted},
		MetricSample{"ridstore_mapping_gc_completed_total", MetricCounter, m.MappingGCCompleted},
		MetricSample{"ridstore_mapping_gc_failed_total", MetricCounter, m.MappingGCFailed},
		MetricSample{"ridstore_mapping_gc_duration_nanoseconds_total", MetricCounter, m.MappingGCDurationNanos},
		MetricSample{"ridstore_mapping_gc_max_duration_nanoseconds", MetricGauge, m.MappingGCMaxDurationNanos},
		MetricSample{"ridstore_mapping_gc_rebuild_nanoseconds_total", MetricCounter, m.MappingGCRebuildNanos},
		MetricSample{"ridstore_mapping_gc_verify_nanoseconds_total", MetricCounter, m.MappingGCVerifyNanos},
		MetricSample{"ridstore_delta_charged_bytes", MetricGauge, m.DeltaChargedBytes},
		MetricSample{"ridstore_delta_reserved_bytes", MetricGauge, m.DeltaReservedBytes},
		MetricSample{"ridstore_delta_soft_limit_bytes", MetricGauge, m.DeltaSoftLimitBytes},
		MetricSample{"ridstore_delta_hard_limit_bytes", MetricGauge, m.DeltaHardLimitBytes},
		MetricSample{"ridstore_mapping_cache_bytes", MetricGauge, m.MappingCacheBytes},
		MetricSample{"ridstore_gc_started_total", MetricCounter, m.GCStarted},
		MetricSample{"ridstore_gc_completed_total", MetricCounter, m.GCCompleted},
		MetricSample{"ridstore_gc_failed_total", MetricCounter, m.GCFailed},
		MetricSample{"ridstore_gc_no_candidate_total", MetricCounter, m.GCNoCandidate},
		MetricSample{"ridstore_gc_copied_bytes_total", MetricCounter, m.GCCopiedBytes},
		MetricSample{"ridstore_gc_reclaimed_bytes_total", MetricCounter, m.GCReclaimedBytes},
		MetricSample{"ridstore_gc_relocated_records_total", MetricCounter, m.GCRelocated},
		MetricSample{"ridstore_gc_skipped_records_total", MetricCounter, m.GCSkipped},
		MetricSample{"ridstore_gc_duration_nanoseconds_total", MetricCounter, m.GCDurationNanos},
		MetricSample{"ridstore_gc_throttled_nanoseconds_total", MetricCounter, m.GCThrottledNanos},
		MetricSample{"ridstore_gc_space_rejections_total", MetricCounter, m.GCSpaceRejections},
		MetricSample{"ridstore_gc_commit_redirects_total", MetricCounter, m.GCCommitRedirects},
		MetricSample{"ridstore_gc_commit_redirect_wait_nanoseconds_total", MetricCounter, m.GCCommitRedirectWaitNanos},
		MetricSample{"ridstore_gc_commit_redirect_admission_nanoseconds_total", MetricCounter, m.GCCommitRedirectAdmissionNanos},
		MetricSample{"ridstore_gc_open_refs_redirected_total", MetricCounter, m.GCOpenRefsRedirected},
		MetricSample{"ridstore_gc_min_free_bytes", MetricGauge, m.GCMinFreeBytes},
		MetricSample{"ridstore_disk_available_estimate_bytes", MetricGauge, m.DiskAvailableEstimateBytes},
		MetricSample{"ridstore_write_stop_free_bytes", MetricGauge, m.WriteStopFreeBytes},
		MetricSample{"ridstore_write_stopped", MetricGauge, m.WriteStopped},
		MetricSample{"ridstore_write_stop_rejections_total", MetricCounter, m.WriteStopRejections},
		MetricSample{"ridstore_disk_space_check_errors_total", MetricCounter, m.DiskSpaceCheckErrors},
		MetricSample{"ridstore_background_checkpoint_requested_total", MetricCounter, m.BackgroundCheckpointRequested},
		MetricSample{"ridstore_background_checkpoint_completed_total", MetricCounter, m.BackgroundCheckpointCompleted},
		MetricSample{"ridstore_background_checkpoint_failed_total", MetricCounter, m.BackgroundCheckpointFailed},
		MetricSample{"ridstore_gc_bytes_per_second", MetricGauge, m.GCBytesPerSecond},
		MetricSample{"ridstore_mapping_gc_publish_nanoseconds_total", MetricCounter, m.MappingGCPublishNanos},
		MetricSample{"ridstore_mapping_gc_max_publish_nanoseconds", MetricGauge, m.MappingGCMaxPublishNanos},
		MetricSample{"ridstore_mapping_gc_conflicts_total", MetricCounter, m.MappingGCConflicts},
		MetricSample{"ridstore_mapping_survey_generation", MetricGauge, m.MappingSurveyGeneration},
		MetricSample{"ridstore_mapping_survey_physical_bytes", MetricGauge, m.MappingSurveyPhysicalBytes},
		MetricSample{"ridstore_mapping_survey_reachable_bytes", MetricGauge, m.MappingSurveyReachableBytes},
		MetricSample{"ridstore_maintenance_automatic_failed_total", MetricCounter, m.MaintenanceAutomaticFailed},
		MetricSample{"ridstore_maintenance_requested_total", MetricCounter, m.MaintenanceRequested},
		MetricSample{"ridstore_maintenance_coalesced_total", MetricCounter, m.MaintenanceCoalesced},
		MetricSample{"ridstore_maintenance_completed_total", MetricCounter, m.MaintenanceCompleted},
		MetricSample{"ridstore_maintenance_failed_total", MetricCounter, m.MaintenanceFailed},
		MetricSample{"ridstore_maintenance_preemptions_total", MetricCounter, m.MaintenancePreemptions},
		MetricSample{"ridstore_maintenance_queued", MetricGauge, m.MaintenanceQueued},
		MetricSample{"ridstore_maintenance_running", MetricGauge, m.MaintenanceRunning},
	)
}

package ridstore

// Metrics is a bounded, non-transactional runtime snapshot. Counters cover the
// current process lifetime; gauges describe the instant at which they were
// sampled. Metrics never authorize recovery, checkpoint, or GC decisions.
type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches         uint64
	Committed, Aborted, Conflicts, CommitUnknown     uint64
	QueueWaitNanos, ValidationNanos                  uint64
	WriteSyncNanos, PublishNanos                     uint64
	DeltaChargedBytes, DeltaReservedBytes            uint64
	DeltaSoftLimitBytes, DeltaHardLimitBytes         uint64
	MappingCacheBytes                                uint64
	DiskAvailableEstimateBytes, WriteStopFreeBytes   uint64
	WriteStopped                                     uint64
	WriteStopRejections, DiskSpaceCheckErrors        uint64
	GCStarted, GCCompleted, GCFailed                 uint64
	GCNoCandidate                                    uint64
	GCCopiedBytes, GCReclaimedBytes                  uint64
	GCRelocated, GCSkipped, GCDurationNanos          uint64
	GCThrottledNanos, GCSpaceRejections              uint64
	GCMinFreeBytes                                   uint64
	BackgroundCheckpointRequested                    uint64
	BackgroundCheckpointCompleted                    uint64
	BackgroundCheckpointFailed                       uint64
	RecordMetaCacheHits, RecordMetaCacheMisses       uint64
	RecordMetaCacheEntries, RecordMetaCacheEvictions uint64
}

type MetricKind uint8

const (
	MetricCounter MetricKind = iota + 1
	MetricGauge
)

const MetricSampleCount = 40

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
		MetricSample{"ridstore_gc_min_free_bytes", MetricGauge, m.GCMinFreeBytes},
		MetricSample{"ridstore_disk_available_estimate_bytes", MetricGauge, m.DiskAvailableEstimateBytes},
		MetricSample{"ridstore_write_stop_free_bytes", MetricGauge, m.WriteStopFreeBytes},
		MetricSample{"ridstore_write_stopped", MetricGauge, m.WriteStopped},
		MetricSample{"ridstore_write_stop_rejections_total", MetricCounter, m.WriteStopRejections},
		MetricSample{"ridstore_disk_space_check_errors_total", MetricCounter, m.DiskSpaceCheckErrors},
		MetricSample{"ridstore_background_checkpoint_requested_total", MetricCounter, m.BackgroundCheckpointRequested},
		MetricSample{"ridstore_background_checkpoint_completed_total", MetricCounter, m.BackgroundCheckpointCompleted},
		MetricSample{"ridstore_background_checkpoint_failed_total", MetricCounter, m.BackgroundCheckpointFailed},
		MetricSample{"ridstore_record_meta_cache_hits_total", MetricCounter, m.RecordMetaCacheHits},
		MetricSample{"ridstore_record_meta_cache_misses_total", MetricCounter, m.RecordMetaCacheMisses},
		MetricSample{"ridstore_record_meta_cache_entries", MetricGauge, m.RecordMetaCacheEntries},
		MetricSample{"ridstore_record_meta_cache_evictions_total", MetricCounter, m.RecordMetaCacheEvictions},
	)
}

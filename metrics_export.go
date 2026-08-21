package ridstore

// MetricKind describes how an external metrics backend should interpret a
// sample. Counters are process-lifetime cumulative values; gauges are current
// snapshots.
type MetricKind uint8

const (
	MetricCounter MetricKind = iota + 1
	MetricGauge
)

const MetricSampleCount = 32

// MetricSample is an allocation-free adapter format for Store.Metrics(). Names
// are stable public identifiers and already include the ridstore namespace and
// base unit.
type MetricSample struct {
	Name  string
	Kind  MetricKind
	Value uint64
}

// AppendMetricSamples appends every bounded ridstore metric in stable order.
// Supplying a destination with MetricSampleCount spare capacity avoids
// allocation. Exporters must not use these observational values as durability
// or GC deletion authority.
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
		MetricSample{"ridstore_gc_insufficient_space_total", MetricCounter, m.GCInsufficientSpace},
		MetricSample{"ridstore_gc_copied_bytes_total", MetricCounter, m.GCCopiedBytes},
		MetricSample{"ridstore_gc_reclaimed_bytes_total", MetricCounter, m.GCReclaimedBytes},
		MetricSample{"ridstore_gc_relocated_records_total", MetricCounter, m.GCRelocated},
		MetricSample{"ridstore_gc_skipped_records_total", MetricCounter, m.GCSkipped},
		MetricSample{"ridstore_gc_duration_nanoseconds_total", MetricCounter, m.GCDurationNanos},
		MetricSample{"ridstore_gc_throttled_nanoseconds_total", MetricCounter, m.GCThrottledNanos},
		MetricSample{"ridstore_disk_available_estimate_bytes", MetricGauge, m.DiskAvailableEstimateBytes},
		MetricSample{"ridstore_write_stop_free_bytes", MetricGauge, m.WriteStopFreeBytes},
		MetricSample{"ridstore_write_stopped", MetricGauge, m.WriteStopped},
		MetricSample{"ridstore_write_stop_rejections_total", MetricCounter, m.WriteStopRejections},
		MetricSample{"ridstore_disk_space_check_errors_total", MetricCounter, m.DiskSpaceCheckErrors},
	)
}

package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		CheckpointFences: 12, CheckpointFenceAcquireNanos: 13, CheckpointFenceHeldNanos: 14, CheckpointFenceMaxHeldNanos: 15,
		CheckpointsStarted: 16, CheckpointsCompleted: 17, CheckpointsFailed: 18, CheckpointDurationNanos: 19, CheckpointMaxDurationNanos: 20,
		CheckpointCaptureWaitNanos: 21, CheckpointMaxCaptureWaitNanos: 22,
		CheckpointCaptureNanos: 23, CheckpointMaxCaptureNanos: 24,
		CheckpointBuildNanos: 25, CheckpointMaxBuildNanos: 26,
		CheckpointPublishNanos: 27, CheckpointMaxPublishNanos: 28,
		CheckpointCaptureConflicts: 29, CheckpointPublishConflicts: 30,
		RecordLogRotations: 31, RecordLogRotationNanos: 32, RecordLogRotationMaxNanos: 33,
		MappingGCStarted: 34, MappingGCCompleted: 35, MappingGCFailed: 36, MappingGCDurationNanos: 37,
		MappingGCMaxDurationNanos: 38, MappingGCRebuildNanos: 39, MappingGCVerifyNanos: 40,
		DeltaChargedBytes: 41, DeltaReservedBytes: 42, DeltaSoftLimitBytes: 43, DeltaHardLimitBytes: 44,
		MappingCacheBytes: 45, GCStarted: 46, GCCompleted: 47, GCFailed: 48, GCNoCandidate: 49,
		GCCopiedBytes: 50, GCReclaimedBytes: 51, GCRelocated: 52, GCSkipped: 53, GCDurationNanos: 54,
		GCThrottledNanos: 55, GCSpaceRejections: 56,
		GCCommitRedirects: 57, GCCommitRedirectWaitNanos: 58, GCCommitRedirectAdmissionNanos: 59, GCOpenRefsRedirected: 60,
		GCMinFreeBytes: 61, DiskAvailableEstimateBytes: 62, WriteStopFreeBytes: 63, WriteStopped: 64,
		WriteStopRejections: 65, DiskSpaceCheckErrors: 66,
		BackgroundCheckpointRequested: 67, BackgroundCheckpointCompleted: 68, BackgroundCheckpointFailed: 69,
		GCBytesPerSecond:      70,
		MappingGCPublishNanos: 71, MappingGCMaxPublishNanos: 72,
		MappingGCConflicts: 73, MappingSurveyGeneration: 74, MappingSurveyPhysicalBytes: 75,
		MappingSurveyReachableBytes: 76, MaintenanceAutomaticFailed: 77,
		MaintenanceRequested: 78, MaintenanceCoalesced: 79, MaintenanceCompleted: 80, MaintenanceFailed: 81,
		MaintenancePreemptions: 82, MaintenanceQueued: 83, MaintenanceRunning: 84,
		MaintenanceQueueWaitNanos: 85, MaintenanceMaxQueueWaitNanos: 86,
		MaintenanceRunNanos: 87, MaintenanceMaxRunNanos: 88,
		MaintenanceRetries: 89, MaintenanceInvariantViolations: 90,
	}
	buffer := make([]MetricSample, 0, MetricSampleCount)
	samples := metrics.AppendMetricSamples(buffer)
	if len(samples) != MetricSampleCount || cap(samples) != cap(buffer) {
		t.Fatalf("len=%d cap=%d", len(samples), cap(samples))
	}
	seen := make(map[string]struct{}, len(samples))
	for index, sample := range samples {
		if sample.Name == "" || sample.Kind != MetricCounter && sample.Kind != MetricGauge {
			t.Fatalf("sample[%d]=%+v", index, sample)
		}
		if _, duplicate := seen[sample.Name]; duplicate {
			t.Fatalf("duplicate metric %s", sample.Name)
		}
		seen[sample.Name] = struct{}{}
		if want := uint64(index + 1); sample.Value != want {
			t.Fatalf("sample[%d]=%+v want value=%d", index, sample, want)
		}
	}
	for name, want := range map[string]MetricSample{
		"ridstore_checkpoint_capture_wait_nanoseconds_total":      {Kind: MetricCounter, Value: 21},
		"ridstore_checkpoint_max_capture_wait_nanoseconds":        {Kind: MetricGauge, Value: 22},
		"ridstore_checkpoint_capture_conflicts_total":             {Kind: MetricCounter, Value: 29},
		"ridstore_gc_commit_redirects_total":                      {Kind: MetricCounter, Value: 57},
		"ridstore_gc_commit_redirect_wait_nanoseconds_total":      {Kind: MetricCounter, Value: 58},
		"ridstore_gc_commit_redirect_admission_nanoseconds_total": {Kind: MetricCounter, Value: 59},
		"ridstore_gc_open_refs_redirected_total":                  {Kind: MetricCounter, Value: 60},
		"ridstore_maintenance_queue_wait_nanoseconds_total":       {Kind: MetricCounter, Value: 85},
		"ridstore_maintenance_max_queue_wait_nanoseconds":         {Kind: MetricGauge, Value: 86},
		"ridstore_maintenance_invariant_violations_total":         {Kind: MetricCounter, Value: 90},
	} {
		found := false
		for _, sample := range samples {
			if sample.Name == name {
				found = sample.Kind == want.Kind && sample.Value == want.Value
				break
			}
		}
		if !found {
			t.Fatalf("missing or invalid sample %s", name)
		}
	}
}

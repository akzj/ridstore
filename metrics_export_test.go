package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		CheckpointFences: 12, CheckpointFenceAcquireNanos: 13, CheckpointFenceHeldNanos: 14, CheckpointFenceMaxHeldNanos: 15,
		CheckpointsStarted: 16, CheckpointsCompleted: 17, CheckpointsFailed: 18, CheckpointDurationNanos: 19, CheckpointMaxDurationNanos: 20,
		RecordLogRotations: 21, RecordLogRotationNanos: 22, RecordLogRotationMaxNanos: 23,
		MappingGCStarted: 24, MappingGCCompleted: 25, MappingGCFailed: 26, MappingGCDurationNanos: 27,
		MappingGCMaxDurationNanos: 28, MappingGCRebuildNanos: 29, MappingGCVerifyNanos: 30,
		DeltaChargedBytes: 31, DeltaReservedBytes: 32, DeltaSoftLimitBytes: 33, DeltaHardLimitBytes: 34,
		MappingCacheBytes: 35, GCStarted: 36, GCCompleted: 37, GCFailed: 38, GCNoCandidate: 39,
		GCCopiedBytes: 40, GCReclaimedBytes: 41, GCRelocated: 42, GCSkipped: 43, GCDurationNanos: 44,
		GCThrottledNanos: 45, GCSpaceRejections: 46,
		GCCommitRedirects: 47, GCCommitRedirectWaitNanos: 48, GCCommitRedirectAdmissionNanos: 49, GCOpenRefsRedirected: 50,
		GCMinFreeBytes: 51, DiskAvailableEstimateBytes: 52, WriteStopFreeBytes: 53, WriteStopped: 54,
		WriteStopRejections: 55, DiskSpaceCheckErrors: 56,
		BackgroundCheckpointRequested: 57, BackgroundCheckpointCompleted: 58, BackgroundCheckpointFailed: 59,
		GCBytesPerSecond:      60,
		MappingGCPublishNanos: 61, MappingGCMaxPublishNanos: 62,
		MappingGCConflicts: 63,
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
	for name, want := range map[string]uint64{
		"ridstore_gc_commit_redirects_total":                      47,
		"ridstore_gc_commit_redirect_wait_nanoseconds_total":      48,
		"ridstore_gc_commit_redirect_admission_nanoseconds_total": 49,
		"ridstore_gc_open_refs_redirected_total":                  50,
	} {
		found := false
		for _, sample := range samples {
			if sample.Name == name {
				found = sample.Kind == MetricCounter && sample.Value == want
				break
			}
		}
		if !found {
			t.Fatalf("missing or invalid sample %s", name)
		}
	}
}

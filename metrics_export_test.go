package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		DeltaChargedBytes: 12, DeltaReservedBytes: 13, DeltaSoftLimitBytes: 14, DeltaHardLimitBytes: 15,
		MappingCacheBytes: 16, GCStarted: 17, GCCompleted: 18, GCFailed: 19, GCNoCandidate: 20,
		GCCopiedBytes: 21, GCReclaimedBytes: 22, GCRelocated: 23, GCSkipped: 24, GCDurationNanos: 25,
		GCThrottledNanos: 26, GCSpaceRejections: 27,
		GCCommitRedirects: 28, GCCommitRedirectWaitNanos: 29, GCCommitRedirectAdmissionNanos: 30, GCOpenRefsRedirected: 31,
		GCMinFreeBytes: 32, DiskAvailableEstimateBytes: 33, WriteStopFreeBytes: 34, WriteStopped: 35,
		WriteStopRejections: 36, DiskSpaceCheckErrors: 37,
		BackgroundCheckpointRequested: 38, BackgroundCheckpointCompleted: 39, BackgroundCheckpointFailed: 40,
		GCBytesPerSecond: 41,
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
		"ridstore_gc_commit_redirects_total":                      28,
		"ridstore_gc_commit_redirect_wait_nanoseconds_total":      29,
		"ridstore_gc_commit_redirect_admission_nanoseconds_total": 30,
		"ridstore_gc_open_refs_redirected_total":                  31,
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

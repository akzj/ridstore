package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		DeltaChargedBytes: 12, DeltaReservedBytes: 13, DeltaSoftLimitBytes: 14, DeltaHardLimitBytes: 15,
		MappingCacheBytes: 16,
		GCStarted:         17, GCCompleted: 18, GCFailed: 19, GCNoCandidate: 20, GCInsufficientSpace: 21,
		GCCopiedBytes: 22, GCReclaimedBytes: 23, GCRelocated: 24, GCSkipped: 25, GCDurationNanos: 26, GCThrottledNanos: 27,
		DiskAvailableEstimateBytes: 28, WriteStopFreeBytes: 29, WriteStopped: 30, WriteStopRejections: 31, DiskSpaceCheckErrors: 32,
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
		want := uint64(index + 1)
		if sample.Value != want {
			t.Fatalf("sample[%d]=%+v want value=%d", index, sample, want)
		}
	}
}

package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		DeltaChargedBytes: 12, DeltaReservedBytes: 13, DeltaSoftLimitBytes: 14, DeltaHardLimitBytes: 15,
		MappingCacheBytes: 16, GCStarted: 17, GCCompleted: 18, GCFailed: 19, GCNoCandidate: 20,
		GCCopiedBytes: 21, GCReclaimedBytes: 22, GCRelocated: 23, GCSkipped: 24, GCDurationNanos: 25,
		GCThrottledNanos: 26, GCSpaceRejections: 27, GCMinFreeBytes: 28,
		DiskAvailableEstimateBytes: 29, WriteStopFreeBytes: 30, WriteStopped: 31,
		WriteStopRejections: 32, DiskSpaceCheckErrors: 33,
		BackgroundCheckpointRequested: 34, BackgroundCheckpointCompleted: 35, BackgroundCheckpointFailed: 36,
		RecordMetaCacheHits: 37, RecordMetaCacheMisses: 38, RecordMetaCacheEntries: 39, RecordMetaCacheEvictions: 40,
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
}

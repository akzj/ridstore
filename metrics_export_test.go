package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		DeltaChargedBytes: 12, DeltaReservedBytes: 13, DeltaSoftLimitBytes: 14, DeltaHardLimitBytes: 15,
		MappingCacheBytes: 16, GCStarted: 17, GCCompleted: 18, GCFailed: 19, GCNoCandidate: 20,
		GCCopiedBytes: 21, GCReclaimedBytes: 22, GCRelocated: 23, GCSkipped: 24, GCDurationNanos: 25,
		DiskAvailableEstimateBytes: 26, WriteStopFreeBytes: 27, WriteStopped: 28,
		WriteStopRejections: 29, DiskSpaceCheckErrors: 30,
		BackgroundCheckpointRequested: 31, BackgroundCheckpointCompleted: 32, BackgroundCheckpointFailed: 33,
		RecordMetaCacheHits: 34, RecordMetaCacheMisses: 35, RecordMetaCacheEntries: 36, RecordMetaCacheEvictions: 37,
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

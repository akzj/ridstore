package ridstore

import "testing"

func TestAppendMetricSamplesMapsEveryFieldInStableOrder(t *testing.T) {
	metrics := Metrics{
		CommitQueued: 1, CommitGroups: 2, GroupBatches: 3, Committed: 4, Aborted: 5, Conflicts: 6, CommitUnknown: 7,
		QueueWaitNanos: 8, ValidationNanos: 9, WriteSyncNanos: 10, PublishNanos: 11,
		DeltaChargedBytes: 12, DeltaReservedBytes: 13, DeltaSoftLimitBytes: 14, DeltaHardLimitBytes: 15,
		GCStarted: 16, GCCompleted: 17, GCFailed: 18, GCNoCandidate: 19, GCInsufficientSpace: 20,
		GCCopiedBytes: 21, GCReclaimedBytes: 22, GCRelocated: 23, GCSkipped: 24, GCDurationNanos: 25, GCThrottledNanos: 26,
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

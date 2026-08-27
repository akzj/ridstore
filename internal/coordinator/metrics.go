package coordinator

import "sync/atomic"

type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches uint64
	Conflicts                                uint64
	QueueWaitNanos, ValidationNanos          uint64
	WriteSyncNanos, PublishNanos             uint64
}

type runtimeMetrics struct {
	commitQueued, commitGroups, groupBatches atomic.Uint64
	conflicts                                atomic.Uint64
	queueWaitNanos, validationNanos          atomic.Uint64
	writeSyncNanos, publishNanos             atomic.Uint64
}

func (m *runtimeMetrics) snapshot() Metrics {
	return Metrics{
		CommitQueued: m.commitQueued.Load(), CommitGroups: m.commitGroups.Load(), GroupBatches: m.groupBatches.Load(),
		Conflicts: m.conflicts.Load(), QueueWaitNanos: m.queueWaitNanos.Load(), ValidationNanos: m.validationNanos.Load(),
		WriteSyncNanos: m.writeSyncNanos.Load(), PublishNanos: m.publishNanos.Load(),
	}
}

func (c *Coordinator) Metrics() Metrics {
	if c == nil {
		return Metrics{}
	}
	return c.metrics.snapshot()
}

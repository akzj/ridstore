package coordinator

import "sync/atomic"

type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches uint64
	Conflicts                                uint64
	QueueWaitNanos, ValidationNanos          uint64
	WriteSyncNanos, PublishNanos             uint64
	CheckpointFences                         uint64
	CheckpointFenceAcquireNanos              uint64
	CheckpointFenceHeldNanos                 uint64
	CheckpointFenceMaxHeldNanos              uint64
}

type runtimeMetrics struct {
	commitQueued, commitGroups, groupBatches atomic.Uint64
	conflicts                                atomic.Uint64
	queueWaitNanos, validationNanos          atomic.Uint64
	writeSyncNanos, publishNanos             atomic.Uint64
	checkpointFences                         atomic.Uint64
	checkpointFenceAcquireNanos              atomic.Uint64
	checkpointFenceHeldNanos                 atomic.Uint64
	checkpointFenceMaxHeldNanos              atomic.Uint64
}

func (m *runtimeMetrics) snapshot() Metrics {
	return Metrics{
		CommitQueued: m.commitQueued.Load(), CommitGroups: m.commitGroups.Load(), GroupBatches: m.groupBatches.Load(),
		Conflicts: m.conflicts.Load(), QueueWaitNanos: m.queueWaitNanos.Load(), ValidationNanos: m.validationNanos.Load(),
		WriteSyncNanos: m.writeSyncNanos.Load(), PublishNanos: m.publishNanos.Load(),
		CheckpointFences: m.checkpointFences.Load(), CheckpointFenceAcquireNanos: m.checkpointFenceAcquireNanos.Load(),
		CheckpointFenceHeldNanos: m.checkpointFenceHeldNanos.Load(), CheckpointFenceMaxHeldNanos: m.checkpointFenceMaxHeldNanos.Load(),
	}
}

func updateMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (c *Coordinator) Metrics() Metrics {
	if c == nil {
		return Metrics{}
	}
	return c.metrics.snapshot()
}

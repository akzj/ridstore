package metrics

import "sync/atomic"

type Runtime struct {
	commitQueued, commitGroups, groupBatches atomic.Uint64
	committed, aborted, conflicts, unknown   atomic.Uint64
	queueWaitNanos, validationNanos          atomic.Uint64
	writeSyncNanos, publishNanos             atomic.Uint64
}

type Snapshot struct {
	CommitQueued, CommitGroups, GroupBatches     uint64
	Committed, Aborted, Conflicts, CommitUnknown uint64
	QueueWaitNanos, ValidationNanos              uint64
	WriteSyncNanos, PublishNanos                 uint64
}

func (m *Runtime) CommitQueued()              { m.commitQueued.Add(1) }
func (m *Runtime) CommitGroup(n int)          { m.commitGroups.Add(1); m.groupBatches.Add(uint64(n)) }
func (m *Runtime) Committed()                 { m.committed.Add(1) }
func (m *Runtime) Aborted()                   { m.aborted.Add(1) }
func (m *Runtime) Conflict()                  { m.conflicts.Add(1); m.aborted.Add(1) }
func (m *Runtime) Unknown()                   { m.unknown.Add(1) }
func (m *Runtime) AddQueueWait(nanos uint64)  { m.queueWaitNanos.Add(nanos) }
func (m *Runtime) AddValidation(nanos uint64) { m.validationNanos.Add(nanos) }
func (m *Runtime) AddWriteSync(nanos uint64)  { m.writeSyncNanos.Add(nanos) }
func (m *Runtime) AddPublish(nanos uint64)    { m.publishNanos.Add(nanos) }

func (m *Runtime) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		CommitQueued: m.commitQueued.Load(), CommitGroups: m.commitGroups.Load(), GroupBatches: m.groupBatches.Load(),
		Committed: m.committed.Load(), Aborted: m.aborted.Load(), Conflicts: m.conflicts.Load(), CommitUnknown: m.unknown.Load(),
		QueueWaitNanos: m.queueWaitNanos.Load(), ValidationNanos: m.validationNanos.Load(),
		WriteSyncNanos: m.writeSyncNanos.Load(), PublishNanos: m.publishNanos.Load(),
	}
}

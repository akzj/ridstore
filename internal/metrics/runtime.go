package metrics

import "sync/atomic"

type Runtime struct {
	commitQueued, commitGroups, groupBatches atomic.Uint64
	committed, aborted, conflicts, unknown   atomic.Uint64
	queueWaitNanos, validationNanos          atomic.Uint64
	writeSyncNanos, publishNanos             atomic.Uint64
	gcStarted, gcCompleted, gcFailed         atomic.Uint64
	gcNoCandidate, gcInsufficientSpace       atomic.Uint64
	gcCopiedBytes, gcReclaimedBytes          atomic.Uint64
	gcRelocated, gcSkipped, gcDurationNanos  atomic.Uint64
}

type Snapshot struct {
	CommitQueued, CommitGroups, GroupBatches     uint64
	Committed, Aborted, Conflicts, CommitUnknown uint64
	QueueWaitNanos, ValidationNanos              uint64
	WriteSyncNanos, PublishNanos                 uint64
	GCStarted, GCCompleted, GCFailed             uint64
	GCNoCandidate, GCInsufficientSpace           uint64
	GCCopiedBytes, GCReclaimedBytes              uint64
	GCRelocated, GCSkipped, GCDurationNanos      uint64
}

func (m *Runtime) CommitQueued()                { m.commitQueued.Add(1) }
func (m *Runtime) CommitGroup(n int)            { m.commitGroups.Add(1); m.groupBatches.Add(uint64(n)) }
func (m *Runtime) Committed()                   { m.committed.Add(1) }
func (m *Runtime) Aborted()                     { m.aborted.Add(1) }
func (m *Runtime) Conflict()                    { m.conflicts.Add(1); m.aborted.Add(1) }
func (m *Runtime) Unknown()                     { m.unknown.Add(1) }
func (m *Runtime) AddQueueWait(nanos uint64)    { m.queueWaitNanos.Add(nanos) }
func (m *Runtime) AddValidation(nanos uint64)   { m.validationNanos.Add(nanos) }
func (m *Runtime) AddWriteSync(nanos uint64)    { m.writeSyncNanos.Add(nanos) }
func (m *Runtime) AddPublish(nanos uint64)      { m.publishNanos.Add(nanos) }
func (m *Runtime) GCStarted()                   { m.gcStarted.Add(1) }
func (m *Runtime) GCCompleted()                 { m.gcCompleted.Add(1) }
func (m *Runtime) GCFailed()                    { m.gcFailed.Add(1) }
func (m *Runtime) GCNoCandidate()               { m.gcNoCandidate.Add(1) }
func (m *Runtime) GCInsufficientSpace()         { m.gcInsufficientSpace.Add(1) }
func (m *Runtime) AddGCCopiedBytes(n uint64)    { m.gcCopiedBytes.Add(n) }
func (m *Runtime) AddGCReclaimedBytes(n uint64) { m.gcReclaimedBytes.Add(n) }
func (m *Runtime) AddGCRelocated(n uint64)      { m.gcRelocated.Add(n) }
func (m *Runtime) AddGCSkipped(n uint64)        { m.gcSkipped.Add(n) }
func (m *Runtime) AddGCDuration(n uint64)       { m.gcDurationNanos.Add(n) }

func (m *Runtime) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		CommitQueued: m.commitQueued.Load(), CommitGroups: m.commitGroups.Load(), GroupBatches: m.groupBatches.Load(),
		Committed: m.committed.Load(), Aborted: m.aborted.Load(), Conflicts: m.conflicts.Load(), CommitUnknown: m.unknown.Load(),
		QueueWaitNanos: m.queueWaitNanos.Load(), ValidationNanos: m.validationNanos.Load(),
		WriteSyncNanos: m.writeSyncNanos.Load(), PublishNanos: m.publishNanos.Load(),
		GCStarted: m.gcStarted.Load(), GCCompleted: m.gcCompleted.Load(), GCFailed: m.gcFailed.Load(),
		GCNoCandidate: m.gcNoCandidate.Load(), GCInsufficientSpace: m.gcInsufficientSpace.Load(),
		GCCopiedBytes: m.gcCopiedBytes.Load(), GCReclaimedBytes: m.gcReclaimedBytes.Load(),
		GCRelocated: m.gcRelocated.Load(), GCSkipped: m.gcSkipped.Load(), GCDurationNanos: m.gcDurationNanos.Load(),
	}
}

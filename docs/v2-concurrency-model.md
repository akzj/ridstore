# ridstore v2 Concurrency Model

状态：Implementation contract

## 1. 数据面原则

Store 不持有覆盖磁盘 I/O、Segment 扫描或 Mapping 重建的全局锁。运行时一致性由不可变文件、单写者顺序、
短 admission fence、generation/epoch 校验和 reader pin 共同建立。

```text
Get
  -> Mapping Delta lookup or pinned immutable Root lookup
  -> RecordLog segment pin + ReadAt

Put
  -> append through the RecordLog single writer
  -> publish final mutation into its private Batch

Commit
  -> Coordinator admission
  -> ordered group fsync
  -> short Mapping Delta publication

Checkpoint / GC
  -> capture a short logical cut
  -> perform all O(N) work without a Store data-plane lock
  -> publish with generation validation
```

## 2. Engine synchronization

| Primitive | Scope | May cross disk I/O | Blocks |
|---|---|---:|---|
| `storeLifecycle.mu` | operation admission and active-operation count | no | one constant-time state transition at API entry/exit |
| `storeState.mu` | fault, open/status registry and recovery snapshot | no | brief state transitions; allocator I/O occurs after unlock |
| `transaction.Batch.mu` | one Batch's state and `Record append -> mutation visible` interval | yes, for that Batch's Put append | only concurrent calls on the same Batch; never unrelated foreground traffic |
| `checkpointRuntime.captureMu` | Checkpoint cut/freeze; Mapping-GC durable publish plus runtime Root owner switch | yes, only for Mapping-GC publication | Checkpoint capture and Mapping-GC publication, never foreground data I/O |
| `MaintenanceScheduler` actor | typed request queue, priority/FIFO, coalescing, dependencies, phase transitions, timers, cancellation and atomic resource grants | no | maintenance workers only; never foreground Get/Put/Commit |
| `PublishCoordinator.mu` | all durable Catalog generation transitions and PublishedState update | yes | metadata publishers and Segment rotation, not reads or non-rotating writes |
| lifecycle root context + `drained`/`done` channels | Close cancellation, operation drain and shutdown completion | no lock is held while waiting | new operations after Close begins |

`CloseContext` atomically rejects new operations, cancels the Store root context, stops and drains Scheduler workers, then
waits on the lifecycle `drained` channel before closing durable components. `Done` is closed only after all owned goroutines
and resources have exited. A caller deadline stops only that caller's wait; shutdown continues under the single lifecycle owner.

Scheduler resources are `heavyIO`, `mappingWriter`, and `recoveryProtocol`. Checkpoint has highest priority and acquires
`mappingWriter`; it deliberately does not wait behind a long, non-preemptible Segment copy. Segment GC holds `recoveryProtocol`, acquires `heavyIO` only for copy/publish/retire phases, and
returns a Checkpoint dependency transition after releasing `heavyIO`. Mapping rebuild holds `recoveryProtocol`; its long COW scan does
not hold `mappingWriter`. Its short durable publication phase explicitly acquires `mappingWriter`; cleanup and reader drain release it again.

Periodic Checkpoint and automatic GC timers belong to the Scheduler actor. Checkpoint pressure, explicit Checkpoint calls,
and Segment/Mapping dependencies all coalesce as the same typed request. No maintenance worker calls the Scheduler or
waits synchronously for another worker.

## 3. Coordinator and Mapping

`Coordinator.admissionMu` covers `ReserveDelta -> queue admission`. Checkpoint holds its write side while it drains the
already admitted queue, appends one durable checkpoint marker, captures recovery metadata and freezes Mapping. It does not
block Get or Record append, and it is released before Root building, SegmentStats I/O or Manifest installation.

Coordinator remains the intentional single durable CommitSeq/fsync writer. Queue backpressure and group fsync serialize
Commit completion, but maintenance copy requests are ordered behind user requests within a formed group.

`Persistent.mu` protects Delta/root pointers and epoch validation. Root reads increment an atomic owner reference while
holding only its read side, then perform Radix I/O with no Mapping lock. Root replacement is a short pointer switch; old
MapStore close and retirement wait on the returned reader-drain channel outside the Mapping lock.

Checkpoint COW construction is optimistic. If Data maintenance or Mapping rewrite advances an incompatible Catalog
dimension before publication, the worker aborts the frozen plan, releases `mappingWriter`, and lets the Scheduler retry
with exponential backoff capped at 64ms until success or caller/lifecycle cancellation. Generation conflicts are scheduling events and never enter the fail-closed path;
transient Mapping plan staleness during capture follows the same retry path. Non-conflict publication failures retain the
existing fail-closed semantics. Mapping GC similarly holds the capture lock through
durable Catalog publication and the runtime Root/MapStore switch, then releases it before waiting for old readers and
retiring the old generation.

## 4. Physical stores

RecordLog has one append writer and per-Segment reader pins. The active Segment separates writer ordering from its short
state lock, so existing Record `ReadAt` calls remain concurrent with append, fsync and rotation I/O. Sealed Segment scans and
reads do not stop appends or reads of other Segments. The old footer fsync also makes its preceding Records durable before
the rotation journal is published. After that journal is durable, publishing the old sealed pathname overlaps creation of
the next Active Segment; Catalog publication and Registry pointer switching still occur only after both files are durable.

MapStore has one `writerMu` for append/rotation/sync ordering. A read resolves its file and immutable valid-end bound under
the short state lock, acquires a reader pin, then performs Node `ReadAt` without that lock. It therefore remains concurrent
with Node append, fsync and rotation. Rotation keeps the old descriptor open across footer write and rename, then takes the
state lock only for the in-memory active/sealed pointer swap. Close rejects new reads and drains reader pins before closing
descriptors.

Catalog serializes Manifest generations across durable install. This can delay another metadata transition such as a rare
Segment rotation, but it does not lock Mapping lookup or RecordLog reads.
The immutable `PublishedState` pointer is loaded without the publisher lock, so background planning and version checks do
not wait for an unrelated Manifest fsync; stale snapshots remain safe because every installation validates its generation
or complete compatible base.

## 5. Versioning and RCU boundary

The durable Catalog keeps one global `Generation`. Individual mutations use
field-level compatibility predicates such as `checkpointBaseCompatible` to
permit a safe rebase over append-only Data rotation without introducing
multiple independently authoritative generations. This is the current
dimension separation: it is validation logic, not additional persisted
version counters.

Do not add Mapping/Data/Recovery generation fields until conflict metrics show
that compatible Catalog churn causes material rebuild waste. Persisted
sub-generations enlarge the crash-state matrix and can recreate the dual-owner
problem the global Catalog generation prevents. New compatibility should first
be expressed and fault-tested as a predicate over one immutable Manifest.

Runtime RCU is already used where it is safe: `PublishedState` is an atomic
immutable snapshot, Mapping Root owners and physical Segment readers use pins,
and retirement waits outside publication locks. RCU cannot replace the
Coordinator fence or durable marker/Catalog transitions because those establish
cross-file persistence order rather than memory lifetime.

## 6. Prohibited regressions

- no Store-wide read/write mutex around Get, Put, Commit, Checkpoint or GC;
- no Mapping lock across Radix disk reads, full-tree walks or fsync;
- no lifecycle lock held while Close waits for active operations or goroutines;
- no Store-global admission lock around Put; sealed-input checks and each Batch mutex must close the append-to-mutation gap;
- no old Mapping file close before its reader count reaches zero;
- no Segment scan, GC pacing, Mapping rebuild, Manifest publish or reader drain inside a Store data-plane lock.

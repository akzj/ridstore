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
| `storeState.mu` | lifecycle, fault, open/status registry and recovery snapshot | no | brief state transitions; allocator I/O occurs after unlock |
| `mutationAdmission.mu` | drains `Record append -> Batch mutation visible` before retirement proof | yes; read side covers one Put append | Put-like calls only; write side is released before Segment scan |
| `checkpointRuntime.captureMu` | Checkpoint cut/freeze; Mapping-GC durable publish plus runtime Root owner switch | yes, only for Mapping-GC publication | Checkpoint capture and Mapping-GC publication, never foreground data I/O |
| `MaintenanceScheduler` actor | typed request queue, priority/FIFO, coalescing, dependencies, phase transitions, timers, cancellation and atomic resource grants | no | maintenance workers only; never foreground Get/Put/Commit |
| `PublishCoordinator.mu` | all durable Catalog generation transitions and PublishedState update | yes | metadata publishers and Segment rotation, not reads or non-rotating writes |
| `activeOps` + `closing` under `storeState.mu` | Close admission and drain | no lock is held while draining | new operations after Close begins |

`Close` does not own the checkpoint capture lock or a maintenance slot while waiting for `activeOps`; the Checkpoint
timer remains alive until admitted operations drain, then Close cancels and drains Scheduler workers before closing storage files.

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
dimension before publication, the worker aborts the frozen plan and rebuilds from the new published generation. Only
exhausted retries or non-conflict failures reach the fail-closed path. Mapping GC similarly holds the capture lock through
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

## 5. Prohibited regressions

- no Store-wide read/write mutex around Get, Put, Commit, Checkpoint or GC;
- no Mapping lock across Radix disk reads, full-tree walks or fsync;
- no lifecycle lock held while Close waits for active operations;
- no retirement proof before draining in-flight Put-to-Batch publication;
- no old Mapping file close before its reader count reaches zero;
- no Segment scan, GC pacing, Mapping rebuild, Manifest publish or reader drain inside a Store data-plane lock.

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
| `Store.mu` | lifecycle, fault, open/status bookkeeping | no | brief entry/exit bookkeeping only |
| `batchSnapshotMu` | Begin/Abort versus checkpoint recovery metadata snapshot | ID reservation during Begin may append | Begin/Abort/checkpoint snapshot; not Get/Put/Commit I/O |
| `mutationFence` | drains `Record append -> Batch mutation visible` before retirement | read side covers one Put append | Put-like calls only; write side is released before Segment scan |
| `checkpointMu` | serializes the Checkpoint worker with Mapping-GC Root publication | yes | maintenance only; data-plane callers wait by generation |
| `maintenanceMu` | serializes Data/Mapping maintenance | yes | maintenance callers only |
| `activeOps` + `closing` | Close admission and drain | no lock is held while draining | new operations after Close begins |

`Close` never owns `checkpointMu` or `maintenanceMu` while waiting for `activeOps`; the Checkpoint worker remains alive
until admitted waiters finish, then Close stops it before closing storage files.

## 3. Coordinator and Mapping

`Coordinator.admissionMu` covers `ReserveDelta -> queue admission`. Checkpoint holds its write side while it drains the
already admitted queue, appends one durable checkpoint marker, captures recovery metadata and freezes Mapping. It does not
block Get or Record append, and it is released before Root building, SegmentStats I/O or Manifest installation.

Coordinator remains the intentional single durable CommitSeq/fsync writer. Queue backpressure and group fsync serialize
Commit completion, but maintenance copy requests are ordered behind user requests within a formed group.

`Persistent.mu` protects Delta/root pointers and epoch validation. Root reads increment an atomic owner reference while
holding only its read side, then perform Radix I/O with no Mapping lock. Root replacement is a short pointer switch; old
MapStore close and retirement wait on the returned reader-drain channel outside the Mapping lock.

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

## 5. Prohibited regressions

- no Store-wide read/write mutex around Get, Put, Commit, Checkpoint or GC;
- no Mapping lock across Radix disk reads, full-tree walks or fsync;
- no lifecycle lock held while Close waits for active operations;
- no retirement proof before draining in-flight Put-to-Batch publication;
- no old Mapping file close before its reader count reaches zero;
- no Segment scan, GC pacing, Mapping rebuild, Manifest publish or reader drain inside a Store data-plane lock.

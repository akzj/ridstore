# SegmentStats 设计

状态：Implemented RecordRef live accounting

## 1. 定位

SegmentStats 用于 GC 候选选择、容量观测和空间收敛判断，不参与 Record 可见性，也不能单独授权删除 Segment。

正确性边界：

```text
Mapping 决定 Record 是否 live
SegmentStats 只提供保守候选提示
GC 删除前必须重新执行精确 Mapping 校验
```

Put/Commit 热路径不读取 Record Header。Mapping entry 自带 `PhysicalSize`，因此 Mapping 在发布变更时可在
同一临界区精确增减 live bytes/records。

## 2. 统计状态

每个 Data Segment 的 checkpoint 统计：

```text
SegmentID
ExactLiveBytesAtCut
ExactLiveRecordsAtCut
```

全局字段：

```text
StatsCoveredCommitSeq
```

它必须与同一 Manifest 中的 Mapping Root `CoveredCommitSeq` 相等。表保存该 Mapping cut 中所有
Data Segment（包括 Active/ReplayStart Segment）的非零项并按 SegmentID 升序排列：

```text
sum(SegmentStats.LiveRecords) == MappingEntryCount
缺失 SegmentStats             => 该 cut 中 live bytes/count 均为 0
```

`StatsKnownForSegment` 仍使用 `segment.end < ReplayStart` 限制 GC：跨越 replay boundary 的 Segment
虽然已有精确 cut 统计，但其 cut 后 WAL 仍可能改变 live 集合，必须等后续 Checkpoint 才能退休。

运行时 Mapping 维护：

```text
SegmentID
LiveBytes
LiveRecords
LastChangedCommitSeq
```

每次成功发布都已从解析计划中持有 OldRef 和 NewRef，因此覆盖、删除和 relocation CAS 可以精确扣减旧值并
增加新值，不产生额外 Mapping lookup 或数据 I/O。失败、冲突和 relocation skip 不改变统计。

## 3. Checkpoint 快照

Mapping Checkpoint barrier 得到 `(C, F, ReplayStart)` 并冻结 Overlay 时，在同一 Mapping 锁内复制 exact live table：

```text
Mapping entries at C + live table at C
=> new Mapping Root R1 at C + SegmentStats(C)
```

Builder 只负责构建 COW Root；Checkpoint 将冻结的完整 live table 按当前 Catalog 文件集校验和排序，
再与新 Root 一起写入同一 Manifest generation。Active Segment 的统计边界是同代 `ReplayStart`；如果它
在 cut 后发生 rotation，统计仍随 SegmentID 保留，而 GC 继续把该 replay-boundary Segment 视为 unknown。

Open 从 Manifest 同时加载 Mapping Root 和完整 live table，然后一次 WAL replay 同步推进 Mapping、Stats、
Batch 状态与水位，不再遍历 Mapping Root 重建 Stats。Offline Verify 仍独立遍历 Mapping 并读取完整
Record，核对 `RecordRef.PhysicalSize` 并重算完整 SegmentStats。

## 4. 原子安装

Checkpoint Manifest 原子安装以下同一 cut 状态：

```text
MappingRootAddr
CoveredCommitSeq = C
CutFrameSeq = F
ReplayStartLogPos
StatsCoveredCommitSeq = C
SegmentStatsTable(C)
```

不能先安装 Root、以后再补同一 C 的统计，也不能把旧 Stats 表标成新 CoveredCommitSeq。Manifest 发布前崩溃继续使用旧 Root+旧 Stats；发布后同时使用新 Root+新 Stats。

统计构建属于后台 Checkpoint 工作，不持有 `publishMu`。只有 cut 指针交换和最终 immutable state 切换使用短锁。

## 5. Commit 与 Recovery

Mapping Publish 对每个实际应用的 mutation 执行：

```text
old exists: live[old.segment] -= old.PhysicalSize / 1 record
new exists: live[new.segment] += new.PhysicalSize / 1 record
```

OldRef 已由 ResolveGroup 取得，NewRef 来自 append 结果或 replay 验证，因此增减不增加随机 I/O。
统计与 Mapping entry 在同一锁内发布；Checkpoint freeze 同时复制二者。Recovery 首先从 Root 重建统计，
随后重放 Commit/Relocation 并走同一 publish 逻辑。

## 6. GC 使用规则

候选估算：

```text
reclaimableBytes = reclaimablePhysicalBytes - ExactLiveBytesAtCut
liveRatio        = ExactLiveBytesAtCut / reclaimablePhysicalBytes
```

Stats 可以决定扫描优先级，不能跳过以下删除门禁：

1. 精确扫描源 Segment；
2. 对每个 PutRecord 验证 `Mapping[ID] == VAddr`；
3. Relocation 后再次确认无 Mapping 指向源 Segment；
4. Mapping Checkpoint 覆盖 Relocation；
5. reader/open-batch/maintenance pin 为 0；
6. Manifest 移除和 rename-to-trash durable。

即使 Stats 显示 live=0，也不能直接删除。

自动候选路径在选择前先做 Checkpoint，并只选择严格落在 ReplayStart 之前的 immutable sealed Segment。
Checkpoint 后不会新增指向候选 source 的 Mapping，因此 cut 时的精确值在选择时也是安全上界；删除仍需
完成 relocation CAS、后置 Checkpoint 和退休证明。

## 7. 资源与 Backpressure

- SegmentStats 表项数量受当前含 live Record 的 Segment 数限制；
- checkpoint 不读取 Data Segment；
- Stats 内存计入 Checkpoint memory budget；
- Checkpoint 失败保留旧 Root、旧 Stats 和 frozen Delta；
- stats build 速度追不上时由总 Overlay hard limit 对新 Commit backpressure，不能丢弃统计或 Delta。

## 8. 验收

- 连续 create、overwrite、delete 后 Stats(C) 与全量 Mapping 遍历一致；
- 同 ID 多次覆盖逐次精确扣减旧 Ref、增加新 Ref；
- Abort、冲突和 CAS 失败副本不计 live；
- Recovery replay 后 live table 与 Mapping 一致；
- Stats/Root Manifest 崩溃点只能同时选择旧代或新代；
- Stats=0 不能绕过 GC 精确校验；
- 长期 checkpoint/GC 后 physical/live/dead 指标收敛。

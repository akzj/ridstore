# ridstore v2 RecordRef 与实时 SegmentStats

状态：Implemented

## 1. 目标

当前 Mapping 保存 `RecordID -> VAddr`。`VAddr` 低三位只是 size class，不能恢复精确
`PhysicalSize`；因此 Mapping 更新时无法直接增减 Segment live bytes，Checkpoint 需要通过
`RecordLog.Inspect` 读取 Record Header。

本变更把唯一运行格式直接升级为：

```text
RecordID -> RecordRef{VAddr, PhysicalSize}
```

项目尚未上线，不提供旧 Mapping 格式迁移、双读或兼容分支。旧格式目录必须返回 unsupported format。

目标：

1. Mapping 的每个 live entry 自包含精确物理长度；
2. Commit、Delete 和 Relocation 发布时可以实时维护每个 Segment 的 live bytes/records；
3. Checkpoint SegmentStats 不再对普通 Mapping entry 执行随机 Header 读取；
4. 为基于死亡速度、稳定轮数和 Segment 年龄的 GC 调度提供数据；
5. 不扩大 Commit/Checkpoint 的长时间全局锁范围。

## 2. 类型与不变量

```go
type RecordRef struct {
    Addr         VAddr
    PhysicalSize uint32
}
```

不变量：

- 零值表示 Mapping 中不存在；
- 非零值要求 `Addr.Valid()`、`PhysicalSize >= RecordHeaderSize`、8-byte 对齐；
- `Addr.MatchesPhysicalSize(PhysicalSize)` 必须成立；
- 相同 `VAddr` 只能对应唯一 `PhysicalSize`，不一致视为 corruption；
- CAS/VersionToken 仍以 `VAddr` 为版本身份，size 不参与用户条件比较；
- Delete 不持久化 tombstone ref，只在 Delta 中表达删除。

## 3. Mapping Node 格式

Radix 仍为固定 9-bit stride、8 层。

- Level 1..7：slot 仍是 8-byte `MapAddr`；
- Level 0：slot 改为 12-byte `(VAddr uint64, PhysicalSize uint32)`；
- Sparse leaf：64-byte bitmap，随后按 slot 顺序编码 packed RecordRef，尾部补零到 8-byte 对齐；
- Dense leaf：512 个连续 RecordRef；
- MapStore Node/Segment format version 直接升级到 3；旧 version 不接受；
- 空 ref 的 canonical encoding 是十二个零字节；半零 ref 非法。

内部节点布局不变，但整个 MapStore 只接受新 Node format，避免在同一代文件中混合两种 leaf。

## 4. Commit 与 Replay

运行时 append 已返回精确 Record physical size。事务准备阶段把新记录表示为 RecordRef，并交给
Mapping proposal。CommitGroup mutation 当前 32 bytes，其中 7 bytes 为 reserved；新格式使用其中
4 bytes 编码 `NewPhysicalSize`，其余 3 bytes 保持为零。Record protocol format version 同步升级到 3，
StoreCatalog minor version 升级到 3；不接受旧目录。

- Put/Relocate 必须携带合法 NewRef；
- Delete 的 NewRef 必须为零；
- Replay 读取 Put 时再次计算并验证 PhysicalSize 与 descriptor 一致；
- ExpectedOld 仍只编码 VAddr，因为当前 Mapping 中的 size 是权威旧值；
- replay publication 与在线 publication 走同一 SegmentStats delta 路径。

## 5. 实时 SegmentStats

Mapping 在成功发布 resolved group 时产生精确的 ref transition：

```text
absent -> new       add(new)
old -> absent       subtract(old)
old -> new          subtract(old), add(new)
CAS skipped         no-op
```

实时表按 SegmentID 保存：

```text
LiveBytes
LiveRecords
LastChangedCommitSeq
```

表与 Mapping publication 在同一临界区更新；任何下溢、溢出或非法 ref 都使 Store fail-closed。
Checkpoint freeze 同时获得一个 covered-sequence 一致的统计视图。Manifest 中的 SegmentStats 仍是
退休授权的 durable 证据；实时表只减少构建成本并服务调度，不能单独授权删除。

## 6. 冷静度采样

每次成功 Checkpoint 后，GC scheduler 对 sealed Segment 的精确统计采样：

```text
DeadBytes       = PhysicalBytes - LiveBytes
NewDeadBytes    = PreviousLiveBytes - CurrentLiveBytes
DeathPerCommit  = NewDeadBytes / CommitSeqDelta
DeathPerSecond  = NewDeadBytes / WallTimeDelta
```

`LiveBytes` 增长会重置稳定轮数。只有无 Open Batch 引用且连续多个样本变化低于阈值的 Segment 才进入
正常 compaction 候选。空间压力可以绕过冷静期，但不能绕过 relocation、Checkpoint 和退休证明。

该历史是有界、非权威的 runtime scheduling state；重启后重新积累样本，只会推迟 GC。

## 7. 多 Segment Compaction

实时死亡速度用于选择相邻、同生命周期的一组 sealed inputs。数据写入独立、完成后立即 seal 的
compaction output，而不是用户 active Segment。输出接入顺序必须是：

```text
output seal + fsync
-> Catalog add outputs
-> durable relocation CAS
-> Checkpoint covers relocation
-> Catalog remove inputs
-> recoverable physical cleanup
```

普通用户 SegmentID 从低位向上分配；compaction output 使用 `[1<<31, MaxUint32]` 的独立命名空间并从
高位向下预留。因此 GC copy 与用户 active 的分配、写队列和 seal 生命周期完全分离。一次 compaction
最多选择 `MaxInputSegments` 个相邻输入，并在一次 Catalog edit 中原子移除全部输入。输出也可以在未来
达到死亡率和冷静期条件后再次参与 compaction，不会成为永久不可回收层。

`journal/DATA-COMPACTION.v3` 记录 reserved、outputs-published、inputs-retired 三个恢复阶段。重启时：未发布
输出直接清理；已发布输出会根据 RecordID 重新建立 bounded relocation CAS；已移除输入只补做物理清理。
所有步骤均以 Catalog membership 判定方向，不依赖不可靠的进程内状态。

## 8. 实施阶段

1. 定义 RecordRef，并升级 Commit mutation codec；
2. 将 Mapping、Radix leaf、Checkpoint builder 和 replay 切换到 RecordRef；
3. 更新 MapStore node format、golden/fuzz/verify；
4. 在 Mapping publication 中维护实时 SegmentStats；
5. Checkpoint 直接冻结统计代际，删除普通路径的 Record Header 随机读取；
6. 增加 death velocity/stable rounds 调度状态；
7. 实现多输入、独立输出的在线 Segment Compaction。

每个阶段必须保持 crash/reopen、CommitUnknown、Checkpoint fault matrix 和 GC retirement 测试通过。

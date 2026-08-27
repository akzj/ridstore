# ridstore v2 Mapping Format

状态：Checkpoint runtime, Delta admission and bounded builder implemented

## 1. 边界

`internal/mapstore` 只保存 immutable radix node。它不知道 RecordID 的事务语义、条件、Delta、
Checkpoint tuple 或 SegmentStats。上层 Mapping 负责 COW；全局 Catalog 负责 live file set。

旧 `internal/format`、`internal/mapping/radix` 的 Format v1 文件不被读取，也不通过 adapter 接入。

## 2. 地址与空树

`MapAddr = uint32 SegmentID | uint32 aligned offset`，0 表示不存在。`MappingRoot=0` 是空树的唯一
表示，空 Node 不落盘。非零 Root 必须指向 live Mapping Segment 中的 Level 7 Node。

## 3. Segment

Mapping 文件位于 `mapping-v2/`：

```text
map-%010d.active
map-%010d.sealed
map-%010d.creating
```

Segment Header 和 Footer 均为 64 bytes。Header 固定 StoreID、SegmentID、PreviousSegment、
SegmentSize；Footer 固定 ValidEnd、FirstNodeSeq、LastNodeSeq、NodeCount。两者都有 CRC32C。

Node 从 offset 64 开始，8-byte 对齐且不得跨 Segment。Manifest 的 sealed entry 只保存
`(SegmentID, ValidEnd)`；Open 必须读取 Footer，验证 identity、边界和逐 Node 扫描结果。

Active Segment 没有 Footer。Open 只能截断最后一个 body 不完整或不足一个 header 的未发布尾部；
坏 magic、坏 CRC、非法 Node 或 Manifest Root 落入待截断区均为 corruption。

## 4. Node

Radix 固定 9-bit stride、8 层：Level 0 保存 tagged RecordLog VAddr，Level 1..7 保存 MapAddr。
Level 7 只允许 slots 0..1。

每个 Node 有 64-byte Header，包含 format version、Level、Encoding、NodeSize、NodeSeq、Prefix、
CoveredCommitSeq、EntryCount、payload CRC 和 header CRC。

- Sparse：64-byte occupancy bitmap + 按 slot 排序的 packed uint64 values；
- Dense：512 个 uint64 slots；
- writer 在 EntryCount `< 504` 时选择 Sparse，否则选择 Dense；
- reader 接受任意 occupancy 的合法 Sparse/Dense，不把 writer threshold 当兼容条件；
- 空 Node 不编码；
- 子 Node 的 CoveredCommitSeq 可以早于 Root，但不能晚于 Manifest cut。

## 5. 已实现与未实现

当前已实现：codec、golden digest、decoder fuzz seed、Segment codec、初始化 active file、顺序 append、
sync、按 Catalog live set Open/Read、sealed 全量验证、active partial-tail repair。

Mapping rotation 使用 durable journal，顺序为 `journal -> seal old -> create new -> Catalog CAS ->
remove journal`；Open 会从 footer 未写、部分写、已完整写、文件已 rename 或 Catalog 已安装的状态继续。
Catalog 并发 generation 改变不会让它改写其他字段，只有 Mapping file-set 前提不变时才重试。

`internal/radix` 已实现 bounded immutable Node Cache、同地址并发 miss 合并、路径 identity 校验和
增量 COW builder。Builder 先按 ID 排序并按 prefix 聚合，因此同一 checkpoint 中每个受影响的
leaf/internal node 最多重写一次；未变化的 subtree 继续引用旧 MapAddr，删除最后一个 slot 会向上剪枝。
Builder 只产生新 Root，不执行 fsync 或发布 Catalog；durability 仍由上层 checkpoint 状态机负责。

`internal/mapping.Persistent` 已实现 v2 运行时基础的 `active Delta + frozen Delta + persistent Root`：

- Lookup 先检查 active/frozen，miss 才进入 radix；
- Root leaf 只保存 VAddr；Lookup 和条件解析均不读取 PutRecord，VAddr 本身就是内部一致性 token；
- ResolveGroup 对 cold read 使用 epoch 重试，不在 Mapping 锁内执行磁盘 I/O；
- PublishGroup 只修改 active Delta，并保持整个 group 的原子可见性；
- Commit 在 Prepare 和 durable Descriptor 之前预留 Delta，Publish 消费实际 entry charge 并退回余量；
- Freeze 原子切换 active Delta，失败/Abort 不丢弃 frozen layer；
- Build 折叠所有待处理 frozen layer，生成并 fsync candidate Root；
- 只有外部 Catalog 已经持久化完整 checkpoint tuple 后，Install 才释放对应 frozen prefix；
- Freeze 后产生的新 Commit 留在新 active Delta，不会混入较早的 candidate Root。

Delta 的 charged/reserved 总量受 hard limit 约束；hard pressure 在 durable 边界前触发 Engine
Checkpoint 并重试。Freeze/Abort 不释放 charge，只有 durable Catalog 已安装且 runtime Root 成功切换后
才释放精确 frozen prefix。当前配置强制 hard limit 所允许的 entry 数不超过 Builder entry budget，避免
Commit 可以被接纳、Checkpoint 却永久无法推进。soft pressure 在 Commit admission 成功并释放
Engine operation read lock 后，通过合并 channel 非阻塞调度后台 Checkpoint。

Checkpoint merge 按 frozen layer 顺序写入一块受 `CheckpointSortBytes` 约束的 mutation 数组，原地稳定
排序并压缩重复 ID；Radix `BuildSorted` 通过固定 8 层 accumulator 流式传播 child change，不再创建
`latest map`、第二份 mutation copy 或逐层 O(N) slice。详见
[v2-checkpoint-builder.md](v2-checkpoint-builder.md)。

Coordinator checkpoint barrier、RecordLog durable cut、Catalog checkpoint tuple、精确 SegmentStats 与
v2 Open/Replay 已接线。Engine 只接受 `mapping.Persistent`；旧的全量内存 Mapping 不再是 Engine
后端，仅待迁移为 Mapping 模型测试 oracle 后删除生产定义。

当前已实现 soft-limit 后台主动调度；显式 Mapping GC 与 MapStore/RecordLog syscall/crash matrix
已经接入。v2 Create 与目录锁已经接入。精确 SegmentStats 在 active segment 未转动时从上一代
表增量应用 folded changes；active 转动后顺序扫描 former-active segment 并与 candidate
Mapping join。需要 metadata 时优先使用
进程内 cache，miss 才由 `RecordLog.Inspect` 读取物理 Header 与 Put protocol header，不读取 Value body。

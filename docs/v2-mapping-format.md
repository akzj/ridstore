# ridstore v2 Mapping Format

状态：Implemented foundation, checkpoint pending

## 1. 边界

`internal/mapstore` 只保存 immutable radix node。它不知道 RecordID 的事务语义、Revision、Delta、
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

尚未实现：bounded Node Cache、COW builder、双 Overlay checkpoint 和 Catalog checkpoint tuple 安装。

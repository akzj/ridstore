# ridstore v2 Manifest

状态：Draft for Review

Manifest 是 v2 唯一的 durable catalog。不存在 RecordLog Manifest、Mapping Manifest 和 GC Manifest
三份镜像状态；所有文件集和 checkpoint 状态通过同一个 generation 原子安装。

## 1. 双槽安装

沿用已经验证的安装协议思想：

```text
encode generation N+1
  -> write MANIFEST.tmp
  -> fsync temp file
  -> rename to inactive MANIFEST slot
  -> fsync store directory
  -> publish in-memory generation N+1
```

具体可使用 `MANIFEST-0`/`MANIFEST-1` 双槽。Open 解码两个槽，选择 generation 最大且完整合法的一份；
generation 相同但内容不同属于 corruption。目录 rename 未完成或 directory fsync 结果不确定时，当前
进程 fail-closed，重新 Open 决定权威 generation。

## 2. Manifest v2 逻辑结构

```go
type ManifestV2 struct {
    Generation            uint64
    StoreUUID             [16]byte
    FormatMajor           uint16
    FormatMinor           uint16
    HardLimits            HardLimitsV2

    RecordLogID           [16]byte
    ActiveDataSegmentID   uint32
    NextDataSegmentID     uint32
    SealedDataSegments    []DataSegmentSummary

    ActiveMapSegmentID    uint32
    NextMapSegmentID      uint32
    SealedMapSegments     []MapSegmentSummary
    MappingRoot           MapAddr
    MappingEntryCount     uint64

    CoveredCommitSeq      uint64
    ReplayStart           LogPos
    ReservedIDHigh        uint64
    ReservedBatchIDHigh   uint64
    IssuedBatchIDHighAtCut uint64
    OpenBatchIDsAtCut     []uint64

    StatsCoveredCommitSeq uint64
    SegmentStats          []SegmentStats
}
```

`NextFrameSeq` 和 `CutFrameSeq` 被删除。RecordLog 的物理顺序由 VAddr/LogPos 表达，Commit 顺序由
CommitSeq 表达。

## 3. HardLimitsV2

以下字段决定磁盘内容能否被安全解析，创建后不可由普通 Open 改变：

```go
type HardLimitsV2 struct {
    SegmentSize          uint64
    MaxValueSize         uint64
    MaxBatchBytes        uint64
    MaxBatchMutations    uint64
    MaxBatchConditions   uint64
    MaxOpenBatches       uint64
    MaxRecordLogPayload  uint64
    IDReserveSize        uint64
    BatchIDReserveSize   uint64
}
```

`MaxRecordLogPayload` 必须由 Protocol 最大 PutRecord 和最大单 Batch Descriptor 推导，并满足
RecordLog Segment 容量。buffer、queue、cache、group target、GC rate 等只影响资源和性能，属于运行时
配置，不写入 HardLimits。

## 4. DataSegmentSummary

```go
type DataSegmentSummary struct {
    SegmentID   uint32
    ValidEnd    uint32
    RecordCount uint64
    FirstAddr   VAddr
    LastAddr    VAddr
}
```

规则：

- 列表按 SegmentID 严格递增，可以因 GC 存在空洞；
- ActiveDataSegmentID 不出现在 sealed 列表；
- 所有 ID 小于 NextDataSegmentID，永不复用；
- ValidEnd、RecordCount、FirstAddr、LastAddr 必须与 sealed Footer 一致；
- 空 sealed Segment 的 FirstAddr/LastAddr 为零；
- Manifest 未列出的 data 文件不是 live set 成员。

Active Segment 的当前 end 不写 Manifest，因为它会持续增长；Open 通过扫描最大完整前缀恢复。

## 5. Mapping 字段

Mapping 文件集与 Data 文件集分开编号。`MappingRoot=0` 是空 Mapping 的唯一表示；此时
`MappingEntryCount=0`，非空 Root 的 count 必须非零。该精确计数与 Root 在同一个 Checkpoint
generation 中安装，供 Mapping GC 在创建 staging 前完成空间 admission。完整 `SegmentStats`（包括
Active/ReplayStart Segment）的 live-record 总和必须等于该计数。Radix checkpoint 在已有 leaf
before/after slots 上计算 count 变化，不增加
全树扫描。空 Node 不为满足
Manifest 约束而落盘。非零 MappingRoot 必须落在 active 或 sealed Mapping Segment 的合法边界内。
Root、CoveredCommitSeq 和 ReplayStart 是一个不可拆分的 checkpoint tuple：

```text
(MappingRoot, MappingEntryCount, CoveredCommitSeq, ReplayStart,
 allocator highs, open batches, SegmentStats)
```

Catalog 不允许分别安装其中一部分。若 checkpoint 失败，整个 tuple 保持旧值。

Checkpoint 安装允许跨越构建期间发生的纯 Data rotation：base Manifest 的 sealed 列表必须仍是当前
列表的精确前缀，新增项必须构成从原 Active 开始的连续 rotation，且所有非 Data-topology 字段保持
不变。Mapping rotation、Data retire 或其他并发安装仍返回冲突，不能用旧快照覆盖新状态。

## 6. ReplayStart

ReplayStart 是不带 size tag 的 `(SegmentID, Offset)`，指向 CheckpointMarker 之后的第一个字节。
它必须：

- 位于 Manifest live data file set；
- offset 8-byte 对齐；
- 不越过 sealed ValidEnd 或恢复后的 active end；
- 对应 CoveredCommitSeq 的 Marker End；
- 不晚于任何未被 MappingRoot 覆盖的 CommitGroupRecord。

如果 ReplayStart 位于已经允许 GC 的 Segment，checkpoint transaction 必须同时保证后续 replay
所需的 Segment 仍在 live set。GC 不能独立移除包含 ReplayStart 的 Segment。

当 ReplayStart 恰好等于 sealed Segment 的 ValidEnd 时，它与下一个 live Segment 的第一个内容
offset 表示同一个空区间边界。Catalog 在移除前者前必须把 ReplayStart 规范化到后者；该变更不
改变 MappingRoot 或 CoveredCommitSeq。若不存在后继 live Segment，则该 Segment 仍不能移除。
RecordLog Scan 按 Manifest 中递增的 live Segment 列表前进，不假设 SegmentID 连续。

## 7. 字段所有权

Catalog 串行化全部安装，但 mutation 权限按操作限定：

| 操作 | 可以修改 |
|---|---|
| RecordLog rotation | ActiveDataSegmentID、NextDataSegmentID、追加一个 SealedData summary |
| Mapping rotation | ActiveMapSegmentID、NextMapSegmentID、追加一个 SealedMap summary |
| Checkpoint | MappingRoot 和完整 checkpoint tuple；允许替换对应 Mapping file set |
| Data GC publish | 移除已证明安全的 SealedData summary、更新对应 stats |
| Mapping GC publish | 移除新 Root 不再引用的 Mapping segments |

普通 mutation 必须携带 expected generation；Checkpoint 携带用于兼容性校验的完整 base Manifest。
Catalog 在锁内复制当前 Manifest、验证调用者只能改变获准字段、完整执行全局 validation，随后安装。
不能向调用者暴露任意 `func(*Manifest)`。

## 8. 初始化

Create 顺序：

```text
1. 创建并 fsync store directory，取得 LOCK
2. 生成 StoreUUID、RecordLogID 和 generation=1 Manifest
3. 把该 Manifest 的规范编码写入 INITIALIZING-v2，fsync + rename + directory sync
4. 幂等创建并 fsync first data active Segment
5. 幂等创建并 fsync first mapping active Segment；初始 MappingRoot=0
6. 安装完全相同的 generation=1 Manifest 并 fsync directory
7. 删除 INITIALIZING-v2 并再次 fsync directory
8. 在仍持有同一 LOCK 时组装并发布 Store
```

Marker 直接保存未来初始 Manifest 的编码，不再维护第二套 UUID/HardLimits schema。Create 重试必须
复用 marker 中的身份，校验已有初始 Segment，清理未发布的 `.creating` 文件并从缺失步骤继续。
Marker 与已安装 Manifest 不完全相同属于 corruption；重试时 HardLimits 不同返回 `ErrConfigMismatch`。

普通 Open 看到 durable marker 返回 `ErrRecoveryRequired`，不能把“Manifest 已 rename、marker 尚未清理”的
目录提前发布。Create 在最终 marker remove 后 directory sync 失败时可以返回错误；此时同进程后续 Open
已可验证完整 Manifest 和两个初始 Segment，崩溃后 marker 若重现则再次由 Create 幂等完成。

Manifest 成功前留下的文件不构成 Store，只有持有目录锁的 Create 恢复路径可以清理本初始化协议明确
命名的临时文件；不得扫描并自动接纳未知文件。

## 9. Open 与 orphan

Open 只接纳 Manifest 引用的文件。其他文件分类为：

- 与合法 journal 匹配：按 journal 恢复；
- `.tmp`/`.creating` 且未发布：可安全清理；
- 已完成但未被 Manifest 引用：orphan，默认隔离并报告；
- ID、UUID 或格式不匹配：corruption，不自动猜测。

不得因为目录中出现更大 SegmentID 就自动提升 Active Segment。

## 10. GC 删除顺序

Manifest 先移除 Segment membership，之后才允许物理删除：

```text
Registry enter Retiring
  -> wait readers
  -> install Manifest without Segment
  -> rename trash
  -> fsync directory
  -> unlink trash
  -> fsync directory
```

Catalog 安装失败时撤销 Retiring。崩溃后：Manifest 已移除但文件仍在是安全 orphan；文件已删除但
旧 Manifest 仍引用它是不允许出现的顺序。因此删除 API 必须要求 Catalog generation/proof，不能只
接收裸 SegmentID。

## 11. 编码

Manifest v2 继续使用带 required bit 的 TLV container：

- 固定 header 包含 magic、format version、generation、StoreUUID、payload length 和 CRC；
- 已知字段必须精确出现一次；
- 未知 required TLV 返回 Unsupported；
- 未知 optional TLV 可以跳过；
- 数组按 count + fixed-size entry 编码；
- decoder 先验证总长和 count 上限，再分配；
- 全局 CRC 覆盖所有 TLV。

Container Header 固定 64 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `RIDMV2\0\0` |
| 8 | 2 | major = 2 |
| 10 | 2 | minor = 0 |
| 12 | 2 | header size = 64 |
| 14 | 2 | reserved |
| 16 | 8 | generation |
| 24 | 16 | StoreUUID |
| 40 | 8 | TLV payload bytes |
| 48 | 4 | payload CRC32C |
| 52 | 4 | header CRC32C |
| 56 | 8 | reserved |

每个 TLV 使用 8-byte Header：`Type uint16 / Flags uint16 / Length uint32`，value 后补零到 8-byte
对齐。`Flags bit0=required`。

| TLV | 内容 |
|---:|---|
| 1 | HardLimitsV2 |
| 2 | RecordLogID |
| 3 | active/next Data SegmentID |
| 4 | sealed DataSegmentSummary 数组 |
| 5 | active/next Mapping SegmentID |
| 6 | sealed MapSegmentSummary 数组 |
| 7 | MappingRoot |
| 8 | checkpoint tuple；末尾 16 bytes reserved，必须为零 |
| 9 | OpenBatchIDsAtCut |
| 10 | StatsCoveredCommitSeq 和 SegmentStats |

TLV 编号和布局已经由 `internal/storecatalog` 的 golden digest 固定，不复用 Format v1 编号。

## 12. 全局校验

Manifest decode 后至少验证：

- UUID/LogID 非零，generation 和所有 next ID 非零；
- active ID 小于 next ID，sealed ID 唯一且严格递增；
- Segment summary 地址、size tag、ValidEnd 和 Footer 一致；
- MappingRoot 为 0，或位于 live Mapping file set；
- ReplayStart 位于 live Data file set；
- StatsCoveredCommitSeq 等于 CoveredCommitSeq，或 stats 明确为空并标记待重建；
- allocator highs 非零且 issued 不超过 reserved；
- open BatchID 唯一、递增并小于 issued high；
- HardLimits 的所有派生尺寸均 checked 且能放入单 Segment；
- 不存在同时 active/sealed/retired 的同一 SegmentID。

## 13. 当前代码映射

- `internal/storecatalog/codec.go`：Manifest v2 codec 和全局校验；
- `internal/storecatalog/install.go`：双槽安装、文件和目录 fsync；
- `internal/storecatalog/catalog.go`：typed Rotation、Checkpoint、Retire mutation；
- `internal/storecatalog/*_test.go`：golden、corruption、fault、race 和 fuzz。

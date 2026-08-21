# 磁盘与二进制格式

状态：Format draft v1；Phase 0 通过后单独冻结

## 1. 格式原则

- 所有整数使用 little-endian；
- CRC 使用 CRC32-C Castagnoli；
- 未知 major version 必须拒绝；
- 未知 Frame Type 必须拒绝，不能跳过后继续写；
- Length 在分配内存和切片前检查硬上限；
- 所有 append Frame 8 字节对齐，padding 必须为 0；
- 磁盘格式不序列化 Go struct 内存布局；
- 文件名用于发现，Manifest 和文件内部 UUID/ID 用于确认身份；
- payload 与 Commit Descriptor 存在同一有序 Data Log，不另写完整 Value WAL。

## 2. 数据目录

```text
<dir>/
  LOCK
  INITIALIZING         # only while Create is incomplete
  CURRENT
  manifests/
    MANIFEST-00000000000000000001
  data/
    DATA-00000001.active
    DATA-00000002.seg
  mapping/
    MAP-00000001.active
    MAP-00000002.seg
  journal/
    MAINTENANCE
    ROTATION
  trash/
  tmp/
```

规则：

- `LOCK` 仅用于 OS 级独占锁，不承载恢复状态；
- `INITIALIZING` 只在首次 Create 尚未完成时存在；
- `CURRENT` 是 ASCII manifest 文件名加换行，最大 128 字节；
- Manifest 文件不可原地修改；
- `data` 保存用户 Record、Commit Descriptor 和系统 Frame；
- `mapping` 保存不可变 Mapping Radix Node；
- `journal/MAINTENANCE` 保存可恢复的 GC/Mapping 安装步骤；
- `journal/ROTATION` 保存短生命周期的 Data Segment rotation 步骤；
- `trash` 中的文件不再参与读取，确认目录状态后异步删除；
- `tmp` 内容在 Open 时按协议清理，不可被当作已提交状态。

Store 不接受身份不明的额外 `.seg` 文件静默加入。发现 Manifest 未列出的正式文件时进入恢复检查或报错。

### 2.1 首次初始化

Create 在获得目录锁后写入 `INITIALIZING` container（Magic `RIDINIT1`，保存 StoreUUID、ConfigHardLimits 和 Phase），按以下顺序推进：

```text
marker Prepared durable
-> create/fsync subdirectories
-> create/fsync DATA-00000001.active header
-> create/fsync MAP-00000001.active header
-> publish generation-1 Manifest and CURRENT
-> fsync root directory
-> remove INITIALIZING and fsync root directory
```

初始 Manifest：Root=0、CoveredCommitSeq=0、CutFrameSeq=0、ReplayStart 指向 Data Segment 1 的 offset 4096、两个 reserved high 和 IssuedBatchIDHighExclusiveAtCut 均为 1、OpenBatchIDsAtCut 为空、NextFrameSeq/NextCommitSeq 均为 1、Next Data/Map Segment ID 均为 2。

Create/Open 看到 marker 时只能按已校验的 StoreUUID、配置和 Phase 幂等完成初始化；如果 CURRENT 已 durable，则只需验证后移除 marker。没有 marker 的非空未知目录绝不自动清空或接管。

## 3. 标识与地址

### 3.1 Store UUID

Store 创建时生成 128-bit UUID。每个 Segment Header 和 Manifest 都保存 UUID，防止把其他 Store 的文件复制进目录后被误读。

### 3.2 Data Segment ID

Data Segment ID 为 `uint32`，从 1 开始，永不复用。0 无效。

Data/Mapping FileID 分配必须使用 checked arithmetic。到达 uint32 上限后禁止创建新 Segment，返回 `ErrAddressExhausted`；GC 删除旧文件也不能使 FileID 重新可用。

### 3.3 VAddr

第一版 VAddr 为 64 bit：

```text
bits 63..32: uint32 segment_id
bits 31..0 : uint32 byte_offset
```

```go
vaddr = uint64(segmentID)<<32 | uint64(offset)
```

约束：

- Segment 最大 4 GiB；
- offset 指向 Frame Header 起点；
- VAddr 0 表示 NotFound；
- Active/Sealed 重命名不改变 Segment ID，因此不改变 VAddr；
- Mapping 文件使用独立的 `MapAddr`，采用相同 `fileID:offset` 编码但类型不互换。

类型系统中 VAddr 与 MapAddr 必须是不同定义类型，避免把 Mapping Node 地址当作用户 Record 地址。

## 4. Segment Header

Data 和 Mapping Segment 都以 4096-byte Header 开始。Header 后第一条 Frame/Node 从 offset 4096 开始。

公共字段：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic：`RIDSEG01` 或 `RIDMAP01` |
| 8 | 2 | MajorVersion |
| 10 | 2 | MinorVersion |
| 12 | 4 | HeaderSize，固定 4096 |
| 16 | 16 | StoreUUID |
| 32 | 4 | FileID |
| 36 | 4 | Flags |
| 40 | 8 | CreatedUnixNano，仅诊断用途 |
| 48 | 8 | FirstFrameSeq/Data，或 FirstNodeSeq/Mapping |
| 56 | 8 | Reserved |
| 64 | 4 | HeaderCRC32C，计算时本字段置 0 |
| 68 | 4028 | Reserved，必须为 0 |

时间戳不参与正确性和排序。

## 5. Data Frame

每个 Data Frame 由固定 64-byte Header、Payload 和 0-padding 组成。

### 5.1 Frame Header

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic：`RDF1` |
| 4 | 2 | MajorVersion |
| 6 | 1 | FrameType |
| 7 | 1 | Flags |
| 8 | 2 | HeaderSize，固定 64 |
| 10 | 2 | Reserved |
| 12 | 8 | TotalSize，含 Header/Payload/Padding |
| 20 | 8 | FrameSeq，全局单调递增 |
| 28 | 8 | BatchID；PutRecord 中表示 OriginBatchID，系统 Frame 可为 0 |
| 36 | 8 | RecordID；非 Record Frame 为 0 |
| 44 | 8 | PayloadSize，不含 padding |
| 52 | 4 | HeaderCRC32C；Header CRC 字段置 0 后计算 |
| 56 | 4 | PayloadCRC32C；空 payload 为 0 |
| 60 | 4 | Reserved |

`TotalSize = align8(64 + PayloadSize)`。解析器同时验证 HeaderSize、TotalSize、PayloadSize、Segment 边界、配置硬上限和零 padding。

BatchID 字段按 FrameType 解释：

- 用户 PutRecord：产生当前逻辑值的 OriginBatchID；
- CommitPart/CommitSeal/BatchAbort：用户 BatchID；
- RelocationPart/RelocationSeal：内部 GC BatchID；
- GC copied PutRecord：保留被复制 Record 的 OriginBatchID；
- allocator/segment system Frame：0。

### 5.2 Frame Type

| Value | Name | Payload |
|---:|---|---|
| 1 | PutRecord | 原始 Value bytes |
| 2 | CommitPart | 一组最终 Mutation Entry |
| 3 | CommitSeal | Commit Descriptor 汇总与 CRC |
| 4 | BatchAbort | Abort 原因码与诊断计数 |
| 5 | IDReserve | 新的 reserved high watermark |
| 6 | SegmentSeal | Data Segment Footer 的镜像摘要 |
| 7 | RelocationPart | GC Mutation Entry |
| 8 | RelocationSeal | GC Relocation 汇总与 CRC |
| 9 | BatchIDReserve | 新的 BatchID reserved high watermark |

Delete 不需要独立 payload Frame；它只出现在 CommitPart 的最终 Mutation Entry 中。

### 5.3 PutRecord OriginBatchID 与 LogicalRevision

PutRecord Payload 是原始用户 Value，`PayloadSize = len(Value)`，可以为 0。PutRecord Header 的 BatchID 字段具有更精确的语义 `OriginBatchID`：它表示最后一次产生该用户逻辑值的用户 Batch，并作为对外 opaque LogicalRevision。

用户 Put 写入 `OriginBatchID = current user BatchID`。GC Relocation 重写 FrameSeq、VAddr、CRC 等物理字段，但必须保留旧 PutRecord 的 OriginBatchID 和 Value。RelocationPart/RelocationSeal 使用独立的内部 GC BatchID，因此 copied PutRecord 的 BatchID 不等于 Relocation Descriptor BatchID。

BatchID 永不复用，所以 OriginBatchID 可防止逻辑版本 ABA。`GetRecord.Revision = Revision(PutRecord.Header.BatchID)`；调用者只能比较相等性，不能依赖其数值或物理含义。条件验证只需读取并校验固定 64-byte PutRecord Header。

## 6. Mutation Entry

CommitPart 和 RelocationPart 的 Payload 由固定 32-byte Entry 组成。

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | RecordID |
| 8 | 1 | Operation：1=Put，2=Delete，3=Relocate |
| 9 | 7 | Reserved |
| 16 | 8 | NewVAddr；Delete 为 0 |
| 24 | 8 | ExpectedOldVAddr；用户 Put/Delete 为 0，Relocate 必填 |

同一 Commit Descriptor 中 RecordID 必须唯一，并按 RecordID 升序排列。Batch 内多次修改同一 ID 在生成 Descriptor 前折叠为最后操作。

CommitPart 只能包含 Put/Delete；RelocationPart 只能包含 Relocate，且 ExpectedOldVAddr 和 NewVAddr 都必须非 0。

Put Entry 指向的 Frame 必须满足：

- FrameType=PutRecord；
- 对用户 Commit，Frame.BatchID 与 Descriptor BatchID 相同；
- Frame.RecordID 与 Entry.RecordID 相同；
- Frame 完整且 Header/Payload CRC 正确；
- FrameSeq 小于对应 CommitSeal FrameSeq。

因此用户 Commit 的 LogicalRevision 等于 Descriptor BatchID。Relocation Entry 的 NewVAddr 则必须指向与 ExpectedOldVAddr 具有相同 OriginBatchID/LogicalRevision 的 PutRecord。

## 7. Commit Descriptor

大量 Mutation 可以分成多个 CommitPart。每个 Part Header 的 BatchID 相同；Payload 为 Mutation Entry 数组。

CommitSeal Payload 固定 64 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | CommitSeq |
| 8 | 4 | PartCount |
| 12 | 4 | MutationCount |
| 16 | 8 | LogicalPayloadBytes |
| 24 | 8 | FirstPartFrameSeq |
| 32 | 8 | LastPartFrameSeq |
| 40 | 4 | DescriptorCRC32C |
| 44 | 4 | CommitFlags |
| 48 | 16 | Reserved |

DescriptorCRC 按 Part 顺序覆盖所有 Mutation Entry 原始字节和 CommitSeal 中除 CRC 外的语义字段。

LogicalPayloadBytes 是该 Batch 最终 Put mutation 的用户 Value bytes 总和，不包含 Frame Header、Descriptor 或 padding。

只有完整 CommitSeal 才代表逻辑提交。CommitPart 本身没有可见性。

CommitSeq 从 1 开始，由 commit coordinator 分配，并严格按 CommitSeal/RelocationSeal 的物理顺序递增。恢复按 CommitSeq/FrameSeq 顺序应用用户 Last-Writer-Wins 或 Relocation CAS。

### 7.1 Relocation Descriptor

RelocationPart/RelocationSeal 使用与 CommitPart/CommitSeal 相同的分 Part、64-byte Seal 和 DescriptorCRC 布局，但 FrameType 不同，所有 Entry 的 Operation 固定为 Relocate。每个 Relocation Batch 从同一 durable BatchID allocator 获得唯一 GC BatchID，Part 和 Seal Header 使用该 GC BatchID；copied PutRecord 保留原 OriginBatchID。RelocationSeal 同样分配全局 CommitSeq，并与用户 CommitSeal 按物理顺序共同形成一个 CommitSeq 序列。

Relocation Entry 的 NewVAddr 必须指向同 RecordID、位于 Seal 之前的完整 PutRecord，并与 ExpectedOldVAddr Record 具有相同 OriginBatchID、PayloadSize、PayloadCRC 和 Value。只有完整且 durable 的 RelocationSeal 才允许恢复或运行时执行 expected-old-VAddr CAS。

## 8. Abort Frame

BatchAbort Payload 固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ReasonCode |
| 4 | 4 | FinalMutationCount |
| 8 | 8 | AppendedPayloadBytes |
| 16 | 8 | LastBatchFrameSeq |
| 24 | 8 | Reserved |

Abort Frame 用于诊断和释放 Open Batch Segment pin。它不需要立即 fsync；没有 CommitSeal 的 Batch 在恢复中始终不可见。

## 9. ID Reserve

IDReserve Payload 固定 24 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | PreviousReservedHighExclusive |
| 8 | 8 | NewReservedHighExclusive |
| 16 | 8 | ReserveGeneration |

IDReserve 作为系统提交单独写入并 durable 后，才允许发放新区间 ID。恢复取所有有效 IDReserve 的最大 high watermark。High watermark 是 exclusive；ID 0 永不发放。

### 9.1 BatchID Reserve

BatchIDReserve Payload 与 IDReserve 一样固定为 24 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | PreviousReservedBatchIDHighExclusive |
| 8 | 8 | NewReservedBatchIDHighExclusive |
| 16 | 8 | ReserveGeneration |

BatchIDReserve durable 后才能从新区间返回 BatchID。恢复取所有有效 BatchIDReserve 的最大 high watermark；BatchID 0 永不发放，也不复用已经预留但未使用的值。

## 10. Segment Seal Footer

Sealed Data Segment 最后写入 4096-byte Footer，然后 fsync、rename 并 fsync `data` 目录。

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic：`RIDEND01` |
| 8 | 4 | SegmentID |
| 12 | 4 | FooterSize，4096 |
| 16 | 8 | ValidDataEndOffset |
| 24 | 8 | FirstFrameSeq |
| 32 | 8 | LastFrameSeq |
| 40 | 8 | FrameCount |
| 48 | 8 | MinCommitSeq，未知为 0 |
| 56 | 8 | MaxCommitSeq，未知为 0 |
| 64 | 4 | FooterCRC32C |
| 68 | 4028 | Reserved |

Active Segment 没有 Footer。Open 可以截断 Active 尾部最后一个不完整 Frame；Sealed Segment 的 CRC/边界错误是 corruption，不能自动截断。

## 11. Mapping Node 格式

Persistent Mapping 使用 9-bit stride、最多 8 层的 copy-on-write radix。每个 Node 保存 512 个 64-bit Slot，Payload 固定 4096 bytes。

Mapping Node 由 64-byte Node Header + 4096-byte Slots 构成，8 字节对齐：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic：`RIDNODE1` |
| 8 | 2 | MajorVersion |
| 10 | 1 | Level：0=Leaf，1..7=Internal |
| 11 | 1 | Flags |
| 12 | 4 | NodeSize，4160 |
| 16 | 8 | NodeSeq |
| 24 | 8 | Prefix |
| 32 | 8 | CoveredCommitSeq |
| 40 | 4 | SlotsCRC32C |
| 44 | 4 | HeaderCRC32C |
| 48 | 16 | Reserved |
| 64 | 4096 | `uint64 Slots[512]` |

Leaf Slot 保存 Data VAddr；Internal Slot 保存 MapAddr；0 表示不存在。

完整 uint64 ID 使用 8 层：最高层只使用 ID 的最高 1 bit，其余层依次使用 9 bit。实现必须拒绝不符合 Prefix/Level 的 Node，防止错误指针形成跨树引用。

### 11.1 Mapping Segment 封口

Mapping Node 只能顺序 append 到当前 `.active` Mapping Segment。文件达到阈值时追加固定 4096-byte Footer，fsync 后 rename 为 `.seg` 并 fsync `mapping` 目录。Footer 格式：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic：`RIDMEND1` |
| 8 | 4 | MapSegmentID |
| 12 | 4 | FooterSize，4096 |
| 16 | 8 | ValidNodeEndOffset |
| 24 | 8 | FirstNodeSeq |
| 32 | 8 | LastNodeSeq |
| 40 | 8 | NodeCount |
| 48 | 4 | FooterCRC32C |
| 52 | 4044 | Reserved |

Active Mapping Segment 没有 Footer。Open 只允许截断 Active 尾部不完整且尚未被当前 Manifest Root 引用的 Node；Sealed Mapping Segment 的边界、Node 或 Footer 损坏均为 corruption。任何 Root 可达 Node 都必须位于已 fsync 的有效范围内。

## 12. Mapping Manifest Root

Manifest 中保存：

```text
MappingRootAddr
CoveredCommitSeq
CutFrameSeq
ReplayStartVAddr
IssuedBatchIDHighExclusiveAtCut
OpenBatchIDsAtCut
```

这些字段是一个原子 checkpoint 状态：Root 包含所有 `CommitSeq <= CoveredCommitSeq` 的 Mapping 和截至 CutFrameSeq 的 allocator/system 状态；恢复从 ReplayStartVAddr（CutFrameSeq 后的精确物理位置）扫描后续 Frame。BatchID cut 字段描述同一 barrier 时的发放和 Open/Committing 集合，用于恢复 Status。

## 13. Manifest

Manifest 使用版本化二进制 TLV，文件以固定 64-byte Header 开始：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic：`RIDMAN01` |
| 8 | 2 | MajorVersion |
| 10 | 2 | MinorVersion |
| 12 | 4 | HeaderSize，64 |
| 16 | 8 | Generation |
| 24 | 16 | StoreUUID |
| 40 | 8 | PayloadLength |
| 48 | 4 | PayloadCRC32C |
| 52 | 4 | HeaderCRC32C，本字段置 0 后计算 |
| 56 | 4 | Flags |
| 60 | 4 | Reserved |

Payload 是连续 TLV，单项格式为 `Type uint16, Flags uint16, Length uint32, Value[Length], zero padding to 8 bytes`。Flags bit 0 表示 required，其余位第一版必须为 0。Type 不得重复；标量 Length 必须精确匹配；数组先保存 `Count uint32 + Reserved uint32`，再保存定长元素。解析时先检查 PayloadLength、单项 Length、Count 乘法和 padding，再分配内存。

第一版 TLV Type：

| Type | Name | Value |
|---:|---|---|
| 1 | FormatVersion | major/minor，各 uint16 |
| 2 | ConfigHardLimits | 8 个 uint64：SegmentSize、MaxValueSize、MaxBatchBytes、MaxBatchMutations、MaxBatchConditions、MaxOpenBatches、IDReserveSize、BatchIDReserveSize |
| 3 | NextDataSegmentID | uint32 |
| 4 | NextMapSegmentID | uint32 |
| 5 | ActiveDataSegmentID | uint32 |
| 6 | SealedDataSegments | FileSummary 数组 |
| 7 | ActiveMapSegmentID | uint32 |
| 8 | SealedMappingSegments | FileSummary 数组 |
| 9 | MappingRootAddr | uint64 MapAddr |
| 10 | CoveredCommitSeq | uint64 |
| 11 | CutFrameSeq | uint64 |
| 12 | ReplayStartVAddr | uint64 VAddr |
| 13 | ReservedIDHighExclusive | uint64 |
| 14 | ReservedBatchIDHighExclusive | uint64 |
| 15 | IssuedBatchIDHighExclusiveAtCut | uint64 |
| 16 | OpenBatchIDsAtCut | uint64 BatchID 数组 |
| 17 | NextFrameSeq | uint64 |
| 18 | NextCommitSeq | uint64 |
| 19 | MaintenanceGeneration | uint64 |

FileSummary 固定 32 bytes：`FileID uint32, Flags uint32, ValidEnd uint64, FirstSeq uint64, LastSeq uint64`。Data 中 Seq 表示 FrameSeq，Mapping 中表示 NodeSeq。数组必须按 FileID 升序、不得重复，且必须与文件 Header/Footer 一致。

必需 TLV：

- FormatVersion；
- ConfigHardLimits；
- NextDataSegmentID；
- NextMapSegmentID；
- ActiveDataSegmentID；
- SealedDataSegments；
- ActiveMapSegmentID；
- SealedMappingSegments；
- MappingRootAddr；
- CoveredCommitSeq；
- CutFrameSeq；
- ReplayStartVAddr；
- ReservedIDHighExclusive；
- ReservedBatchIDHighExclusive；
- IssuedBatchIDHighExclusiveAtCut；
- OpenBatchIDsAtCut；
- NextFrameSeq；
- NextCommitSeq；
- MaintenanceGeneration。

`NextFrameSeq`、`NextCommitSeq` 和两个 reserved high watermark 都是永不回退的分配下界，不表示所有更小序号都一定存在。恢复扫描 ReplayStart 之后的有效 Frame，并以 `max(manifest value, scanned durable value + 1)` 恢复下一序号；允许因崩溃留下空洞，不允许复用。

`IssuedBatchIDHighExclusiveAtCut` 是 checkpoint barrier 时已经由用户 `Begin` 或内部 Relocation allocator 取用的最大连续 BatchID 上界。`OpenBatchIDsAtCut` 是该时刻仍为 Open/Committing 的用户 BatchID 排序数组，数量不得超过持久化的 `MaxOpenBatches` 硬限制；maintenance coordinator 保证建立 checkpoint cut 时没有未完成的 Relocation Batch。二者只用于恢复 Batch Status：切点前已经结束且不在保留状态索引中的 Batch 返回 `ErrStatusExpired`；切点时开放或切点后可能发放的 Batch，可以由 ReplayStart 后的 Seal/Abort 或其缺失确定结果。

Checkpoint 构建期间 Data/Mapping 文件集合仍可能因 append rotation 而变化。安装新 Root 时必须持有 Manifest 安装串行器，读取最新 durable 文件目录，再把新的 `(MappingRootAddr, CoveredCommitSeq, CutFrameSeq, ReplayStartVAddr, IssuedBatchIDHighExclusiveAtCut, OpenBatchIDsAtCut)` 合并进去生成完整新 Manifest。不得从 Checkpoint 开始时保存的旧 Manifest 整体覆盖当前文件集合。Mapping Checkpoint、Mapping GC 和 Data GC 的 Manifest 安装都经过同一个串行器，并用 generation CAS 拒绝基于过期 generation 的安装。

未知 optional TLV 可跳过；未知 required TLV 必须拒绝。

Manifest 发布顺序：

```text
write tmp manifest
-> fsync file
-> rename to MANIFEST-generation
-> fsync manifests directory
-> write tmp CURRENT
-> fsync tmp CURRENT
-> rename CURRENT
-> fsync root directory
```

旧 Manifest 在新 CURRENT durable 后才能进入 trash。Open 在 CURRENT 损坏时可以扫描 Manifest generation，但只有 CRC、UUID、文件引用全部成立的最高 generation 才可作为恢复候选；自动选择必须记录诊断。

## 14. Maintenance Journal

GC/Mapping 安装使用单个版本化 Journal，状态至少包括：

```text
OperationID
OperationType
Phase
SourceFiles
DestinationFiles
OldManifestGeneration
NewManifestGeneration
CRC
```

`MAINTENANCE` 使用与 Manifest 相同的 64-byte container header 和 TLV framing，Magic 改为 `RIDJNL01`；Header Generation 是 journal generation。required TLV 固定为：OperationID 16 bytes、OperationType uint16、Phase uint16、SourceFiles FileSummary array、DestinationFiles FileSummary array、OldManifestGeneration uint64、NewManifestGeneration uint64。OperationType 第一版为 1=DataGC、2=MappingCheckpoint、3=MappingGC；各协议列出的 Phase 从 1 开始按顺序编号，只允许保持当前 phase 或前进到下一 phase，未知值拒绝 Open。

Journal 同样采用 temp+fsync+rename+directory fsync。每个 Phase 必须幂等；Open 根据 Phase 继续或回滚文件安装，不能依靠文件名猜测。Phase 更新必须完整重写 Journal container，不原地覆盖字节。

Data rotation 使用独立 `ROTATION` journal，采用相同 container header，Magic 为 `RIDROT01`，Payload 固定 32 bytes：OldSegmentID uint32、NewSegmentID uint32、BaseManifestGeneration uint64、InstalledManifestGeneration uint64、Phase uint32、Reserved uint32。Phase 为 1=Prepared、2=OldSealed、3=OldRenamed、4=NewCreated、5=ManifestInstalled。它与长时间 GC/Mapping 操作分开，避免 Active Segment 满时等待整个维护任务结束；两者最终安装 Manifest 时仍由同一个安装串行器协调。

## 15. 格式冻结门禁

Format v1 冻结前必须具备：

- 每种 Frame/Node/Manifest 的 golden bytes；
- big/little-endian 显式测试；
- 所有 Length、Count、Offset 的溢出测试；
- decoder fuzz；
- unknown version/type/TLV 测试；
- torn Header/Payload/Footer 测试；
- CURRENT/Manifest rename 崩溃矩阵；
- Active 可截断、Sealed 不可截断的测试；
- 不同 Go 版本生成相同 golden bytes。

冻结后任何不兼容修改必须提升 major version 并提供离线迁移工具；不能在 Open 时静默改写旧格式。

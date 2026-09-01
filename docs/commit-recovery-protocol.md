# Commit 与 Recovery 协议

状态：Development contract v1

> 本文描述 Format v1 的 Frame/CommitSeal 协议。v2 使用单 CommitGroupRecord，条件只比较 Mapping
> VAddr，不读取 PutRecord Header 恢复 Revision；见 [v2 总体架构](v2-architecture.md)、
> [v2 Record Protocol](v2-record-protocol.md) 与 [v2 API 契约](v2-api-contract.md)。

## 1. 设计选择

第一版采用：

- 一个 Store 内单一 append sequencer 决定全局 Frame 顺序；
- 不同 Open Batch 的 PutRecord 允许物理交错；
- Put 时直接 append payload，不在 Batch 内存保存完整 Value；
- Batch 内存只保存最终 Mutation 元数据和已写 Record VAddr；
- Commit 时重新生成完整、按 ID 排序的 Commit Descriptor；
- 多个 Commit Descriptor 合并 write，并共享一次 fsync；
- Mapping 按 CommitSeal 顺序发布；
- Commit Descriptor 自包含最终 ID→VAddr/Delete 集合，恢复不依赖在扫描中长期保存所有 Open Batch。

该选择允许大 Value 和并发 Batch，同时把恢复内存限制为 Commit Descriptor 大小，而不是所有未提交 payload 大小。

## 2. 运行时组件

```text
Batch Producers
    │ Put/Delete/Commit
    ▼
Append Sequencer ──> Active Data Segment
    │
    ▼
Commit Coordinator ──> group write/fsync
    │
    ▼
Mapping Publisher
```

职责：

- Batch：维护状态、配额和每个 ID 的最后操作；
- Append Sequencer：分配 FrameSeq/VAddr，保证全局物理顺序；
- Commit Coordinator：生成 CommitSeq、合并 Commit Descriptor、执行 fsync；
- Mapping Publisher：在 fsync 成功后按 CommitSeq 原子发布；
- Segment Registry：记录 Open Batch 对 Segment 的引用和 Reader pin。

## 3. Put

Put 流程：

```text
1. 检查 Batch=Open、ID、Value 和配额
2. 提交 Put append request
3. append sequencer 分配 FrameSeq
4. 构造 `OriginBatchID=current BatchID` 的 PutRecord Header、CRC、Payload 和 Padding
5. 顺序写入 Active Segment
6. 返回 VAddr
7. Batch 将 ID 的最终 Mutation 更新为 Put(VAddr)
8. 若 ID 之前已有本 Batch VAddr，旧 VAddr 成为 dead candidate
```

Put 返回不表示 durable，也不表示对 Get 可见。

出现 short write 或无法确认的 append 错误时：

- 停止 append sequencer；
- Batch 标记 Failed；
- Store 进入只读故障状态；
- 不继续使用可能存在 torn tail 的 Active Segment；
- 通过重新 Open 执行尾部恢复。

## 4. Delete

Delete 不立即写 payload Frame：

```text
Batch.finalMutation[ID] = Delete
```

Commit Descriptor 中的 Delete Entry 才是持久化删除意图。Delete 一个不存在的 ID 第一版允许提交，并产生幂等 NotFound 状态；这使恢复和重试更简单。

## 5. Allocate

Store 在内存中维护 `[nextID, reservedHigh)`。

当区间耗尽：

```text
newHigh = reservedHigh + IDReserveSize
append IDReserve system frame
fsync
publish reservedHigh = newHigh
```

加法必须使用 checked arithmetic。`newHigh` 超过 uint64 空间时永久返回 `ErrIDExhausted`，不能回绕到低值。

IDReserve 可以与用户 Commit group fsync，但在对应 fsync 成功前不能向调用者返回新区间 ID。

Reserve fsync 结果不确定时 Store 进入 CommitUnknown 等价故障状态；恢复取 durable IDReserve 最大值，绝不回退发放。

BatchID 使用独立的 `[nextBatchID, reservedBatchIDHigh)` 区间和 BatchIDReserve system frame。`Begin` 在区间耗尽时必须先持久化新的 high watermark，之后才返回 BatchID。BatchIDReserve 可以参加 group fsync，但其 fsync 不确定时 Store 同样 fail closed；恢复后跳过整个已持久化但尚未使用的区间也可以，绝不能复用。BatchID 预留同样使用 checked arithmetic，耗尽时返回 `ErrIDExhausted`。

## 6. Commit 准备

Batch 从 Open CAS 到 Committing 后：

1. 禁止新的 Allocate/Put/Delete/Abort；
2. 折叠为每个 ID 一个最终 Mutation；
3. 最终 Mutation 按 ID 升序排序；
4. 折叠并校验条件，按条件 ID 升序排序；
5. 校验每个 Put VAddr 指向本 Batch、OriginBatchID=BatchID 的完整 PutRecord；
6. 计算 Mutation Count、Logical Payload Bytes；
7. 将最终 Mutation、条件和 payload VAddr 交给 coordinator。

空 Batch 的 Commit 合法：它仍获得 BatchID/CommitSeq，但不改变 Mapping。实现可以用无 Part 的 CommitSeal 表达。

### 6.1 条件验证

Commit Coordinator 是条件验证的唯一串行化点。它按准备提交的确定顺序维护只在当前 group 内存在的 virtual Mapping：

Coordinator 在已经形成的 group 内先做稳定分区：UserCommit 在 Relocation 之前；UserCommit 之间和 Relocation 之间各自保持入队 FIFO。该优先级不跨 group，也不越过 Checkpoint barrier，并且不改变 group 容量、durability/fsync 边界。它使同组用户条件提交先观察旧 VAddr，随后过期的 Relocation 通过 expected-old-VAddr CAS skip，减少 GC 引起的可避免冲突。

```text
virtual = current committed Mapping
for request in stable(UserCommit first, Relocation second) group order:
    read every condition from virtual
    if any condition fails:
        mark Batch Aborted with ErrConflict
        do not assign CommitSeq and do not emit CommitPart/CommitSeal
    else:
        admit Batch
        apply its final mutations to virtual
```

因此同组两个 `ExpectAbsent(ID)` 只有第一个可以通过，后一个看到第一个的 virtual mutation 后冲突。Blind Batch 没有条件，始终进入 virtual Mapping 并保持 Last-Writer-Wins。

若 group 中包含内部 Relocation，virtual Mapping 也按全局顺序执行 expected-old-VAddr CAS，并为每条 Entry 生成 immutable apply/skip plan。该阶段允许在不持 `publishMu` 时读取冷 Root；从验证开始到本 group Publish 完成，Coordinator 不允许下一 group 或 Checkpoint barrier 越过。Relocation 成功与否不改变 LogicalRevision；fsync 后 Publisher 只执行 resolved plan，不在短发布锁内重新做可能触发 I/O 的 Lookup。Recovery 按同一 CommitSeq 从 Descriptor 重算 CAS，结果必须一致。

Persistent Mapping 只保存 VAddr；验证 ExpectRevision 时按 Get 相同的 `Lookup -> SegmentRegistry.Acquire -> revalidate Mapping -> read` 协议 pin 当前 Segment，再读取固定 64-byte PutRecord Header，将 OriginBatchID 解释为 LogicalRevision并校验 Header CRC，最后释放 pin。该冷读是 conditional commit 主动选择的成本，不能转移到 Blind Put 热路径。验证期间 coordinator 不允许下一 group 越过，也不允许 Checkpoint barrier 插入；验证完成到对应 group Publish 之间的提交顺序不可改变。

Blind Batch 不读取旧 Mapping 对应的 Record Header，也不参与 Revision 比较；它只按提交顺序覆盖 Mapping。

条件不是 durable mutation，不写入 Commit Descriptor。只有验证成功的 Batch 才产生 Seal；Recovery 直接重放这些 Seal，不重新判断历史条件。条件失败后已经 append 的 PutRecord 没有 Seal，按未提交垃圾处理。

## 7. Group Commit

Commit Coordinator 单 goroutine 运行。收到第一个 request 后，在以下任一条件满足时形成 group：

- 当前队列暂时为空；
- 达到 `MaxGroupBytes`；
- 达到 `MaxGroupBatches`；
- 达到短暂的 `MaxGroupDelay`；
- 需要 Segment rotation。

第一版默认不主动 sleep 等待更多请求；fsync 自身形成自然 batching window。`MaxGroupDelay` 默认 0，仅作为经过基准证明后的可配置优化。

在进入第 6.1 节不可穿插 barrier 的验证区间前，先按 `runtime-config.md` 为所有最终 mutation/Relocation 成功的上界预留 active Delta + Stats additions charge；超过 hard limit 的请求在此等待，Checkpoint 仍可推进。Context 取消属于确定 Aborted，reservation 必须归还。随后验证 request、移除冲突 Batch并释放其 reservation；Relocation CAS skip 的多余 charge 同样释放。对每个已验证且保有 reservation 的 admitted request：

reservation 按 queue order 获取；已有部分 admitted group 时，不得持有其 reservation 等待下一个无法预留的请求，而应立即执行当前 group并把后者留在队首。只有空 group 的队首无法预留时才允许等待 Checkpoint。

```text
append CommitPart(s)
append CommitSeal with next CommitSeq
```

整个 group 一次 write，随后一次 `fdatasync/fsync`。CommitSeal 的物理顺序就是 CommitSeq 顺序。

append sequencer 始终为最终 128-byte SegmentSeal Frame 和 4096-byte Footer 预留空间。若 group 总大小加该保留区将跨越 Segment 上限，先 Seal/Sync 当前 Segment并创建新 Active Segment，再把完整 Descriptor 写入新 Segment。单个 Descriptor 可以分 Part，但 CommitPart 与 CommitSeal 不跨 Data Segment，简化恢复。

## 8. Commit 确认与 Mapping 发布

fsync 成功后，coordinator 按 CommitSeq 顺序处理每个 Batch：

```text
Mapping.Publish(all final mutations)
mark Batch Committed
reply CommitResult
```

Mapping Publish 必须全批可见。Publish 是内存操作，不执行磁盘 I/O。

Descriptor 一旦允许落盘，其 Delta reservation 已不可撤销地保证 Publish 容量；fsync 成功后的 Publish 不得再被 Delta hard limit 阻塞。Publish 完成后 reservation 转为 active Delta charge。若在落盘前 group 构建失败，必须归还 reservation。

如果 fsync 成功但进程在 Publish/Reply 前崩溃，恢复会从 CommitSeal 重建 Mapping，因此 Batch 仍为 Committed。

如果某个 Batch 的 Mapping Publish 因内部错误失败，Store 立即 fail closed；不得向后续 Batch返回成功。重启恢复按 durable CommitSeq 重新发布全部 Batch。

## 9. Commit 错误分类

### 9.1 确定未提交

CommitSeal 尚未交给 append sequencer且错误明确发生在准备阶段：

- 返回具体错误；
- Batch 进入 Failed/Aborted；
- Mapping 不变。

条件验证失败属于本类：返回 `ErrConflict`，Batch 进入 Aborted，不生成 CommitSeq 或 CommitSeal。即使 BatchAbort marker 随后丢失，也不能恢复为 Committed。

### 9.2 确定提交

fsync 成功且 Mapping Publish 完成：返回 CommitResult。

### 9.3 CommitUnknown

以下情况返回 `ErrCommitUnknown`：

- write 可能已部分完成；
- fsync 返回错误或被底层中断；
- Context 在 CommitSeal 提交后取消；
- 进程/设备状态使 Library 无法证明 Seal 是否 durable；
- reply 丢失但后台结果无法同步交付。

CommitUnknown 后：

- BatchID 保持可查询；
- Store 停止接受新写；
- 调用者不能重用 ID 表达另一对象；
- 重新 Open 后 `Status(BatchID)` 根据 durable CommitSeal 返回 Committed 或 Aborted/Unknown；
- 相同 BatchID 的自动重放不在第一版 API 内，避免重复 payload 写入语义不清。

## 10. Abort

Open Batch Abort：

```text
CAS Open -> Aborted
append BatchAbort（best effort）
释放 Open Batch Segment 引用
清空内存 Mutation 元数据
```

Abort Marker 不要求立即 fsync。即使 Marker 丢失，没有 CommitSeal 的 Record 也不会恢复为可见数据。

Failed Batch 必须 Abort 或由 Close 清理。Committing/Committed Batch 不能 Abort。

## 11. Checkpoint Cut

Mapping Checkpoint 通过 append/commit coordinator 中的 barrier 请求捕获一致的三元组，并在同一个临界阶段完成 active Delta 切换：

```text
(CoveredCommitSeq=C, CutFrameSeq=F, ReplayStart=position after F)
```

Barrier 请求排在 Commit publish 序列中。轮到它时，coordinator 先确定最近一次成功 fsync 的物理边界 F；如需要推进 C，则先完成一次 fsync。随后持有 `publishMu`，阻止更晚 Commit 发布，得到 C 并交换 active Delta，最后才释放 barrier。Barrier 保证：

- 所有 `CommitSeq <= C` 已完成 Mapping Publish；
- 这些 Commit Descriptor 的 FrameSeq 都 `<= F`；
- checkpoint 捕获的 ID reserve/allocator 元数据覆盖所有 `FrameSeq <= F` 的 durable system frame；
- barrier 之后的新 FrameSeq 都 `> F`；
- ReplayStart 精确位于 F 后，不依赖“寻找下一条 Commit”。
- F 是已确认 durable 的连续物理前缀，不能指向仅 write、尚未 fsync 的 Active 尾部；
- captured/frozen 中不包含 `CommitSeq > C` 的 mutation。
- 同时捕获 `IssuedBatchIDHighExclusiveAtCut` 和排序后的 `OpenBatchIDsAtCut`。Coordinator barrier 会先完成此前
  admission 的 Commit；因此后者只包含 barrier 返回后仍为 Open/Failed 的 Batch。已 durable 但调用方尚未
  消费结果、尚未来得及从进程内 open map 移除的 terminal Batch 必须排除。

随后：

```text
1. barrier 在 `publishMu` 内原子交换 Delta Overlay
2. captured overlay 只包含 CommitSeq <= C 的更新
3. 释放 barrier 后新 Commit 才能进入 fresh overlay
4. 后台构建新 Persistent Mapping Root，并由 base Root 到 cut-final Mapping 批量生成精确 SegmentStats(C)
5. fsync Mapping files
6. 通过 Manifest 安装串行器原子发布 Manifest(root, C, F, replayStart, StatsCoveredCommitSeq=C, SegmentStats(C))
```

第 6 步必须以安装时最新的 durable Data/Mapping 文件集合为基底，只替换同代 Mapping Root、checkpoint cut 与 SegmentStats 字段；不能用切点时缓存的旧 Manifest 覆盖并发 rotation 的结果。Root 或 Stats 任一构建/校验失败都不安装该 generation，并保留旧 Root/Stats 与 frozen Delta。

Open Batch 的 payload 可以早于 F，但其 Commit Descriptor 只在未来 Commit 时生成并位于 F 之后。Descriptor
自包含最终 payload 的 VAddr，因此恢复从 ReplayStart 扫描仍能回读并验证 payload。若 Data GC 已复制该
未提交 Put，Descriptor 可以引用已 seal、fsync 且进入 Catalog 的高位 Compaction Segment；Replay 对普通
日志地址证明 `NewAddr < CommitAddr`，对高位地址则通过 Catalog reader 验证完整 Put identity。

Checkpoint 不需要等待 Open Batch 结束。GC 会复制它位于 sealed source 的最终 Put，在 Coordinator
顺序流安装临时地址重定向，并原地改写尚未 Prepare 的 mutation。已 Prepare 的 Commit 在 descriptor
编码前由 Coordinator 转换，因此不需要在排空旧 Commit 队列期间停止新 admission。

## 12. Open/Recovery 总流程

```text
1. 获取目录锁
2. 读取有效 CURRENT 和 Manifest；以 CURRENT 为发布权威，删除并 fsync 可证明未发布的 `.CURRENT.tmp`、合法 `MANIFEST-*.tmp` 与 generation 高于 CURRENT 的 orphan final Manifest；保留当前和更老 generation
3. 校验 Store UUID、格式、硬限制、文件集合及 sealed Header/Footer/terminal Seal envelope
4. 恢复或完成 Maintenance Journal
5. 从同一 Manifest checkpoint 打开 Persistent Mapping Root，并加载
   `StatsCoveredCommitSeq == CoveredCommitSeq` 的完整 SegmentStats Base（包含 Active/ReplayStart Segment）
6. 从 ReplayStart 所在 Segment 开始扫描 Data Frames；更早 sealed Segment 不做启动全扫
7. 验证 Frame/Commit Descriptor
8. 按 CommitSeq 重放 Mapping；每次成功发布同时从 Stats 扣除 OldRef 并加入 NewRef
9. 恢复 IDReserve、BatchIDReserve、FrameSeq、CommitSeq high watermark
10. 修复 Active Segment torn tail
11. 重建 Segment live/open/pin 元数据
12. replay Descriptor 引用的 Record 按 VAddr 随机读取并校验；历史 Mapping Record 在 Get/GC/Verify 访问时校验
13. 创建/恢复 Active Segment
14. 开放 API
```

任何一步无法证明安全都返回错误，不开放部分写服务。

## 13. Frame 扫描

Replay 扫描规则：

- 从 Segment Header 后开始；
- 每次先读取固定 Header 并验证 Header CRC；
- 检查 TotalSize/PayloadSize/对齐/上限；
- 再读取 Payload 并验证 CRC；
- FrameSeq 必须严格递增；
- Sealed Segment 必须精确到 Footer.ValidDataEnd；
- Active Segment 最后一个不完整 Frame 可以截断；
- Active 中间位置损坏是 corruption；
- Sealed 任意损坏是 corruption。

普通 Open 不为了发现与 replay/当前访问无关的历史损坏而顺序扫描所有旧 sealed payload。它先验证每个 immutable 文件的 Header、Footer 和固定大小 terminal SegmentSeal；ReplayStart 之前、被新 Descriptor 引用的 PutRecord 通过 VAddr 随机读取并验证完整 Frame CRC。Get/GC 同样在使用时验证，offline Verify/Scrub 负责全文件扫描。这个边界只延迟无关历史 corruption 的发现，不允许返回未通过 CRC 的 Value。

未知 FrameType/major version 立即停止并返回 `ErrUnsupported`。

## 14. Commit/Relocation Descriptor 重放

恢复不根据 PutRecord 出现就更新 Mapping。只有读到完整 CommitSeal 或 RelocationSeal 后：

1. 收集同一 Segment 中对应 CommitPart；
2. 验证 PartCount、FrameSeq 范围、MutationCount 和 DescriptorCRC；
3. 对每个 Put Entry 验证目标 PutRecord；
4. 用户 Commit 验证 OriginBatchID=Descriptor BatchID；Relocation 验证新旧 OriginBatchID 相同；两者都验证 RecordID、CRC 和 Record FrameSeq < Seal FrameSeq；
5. 确认 CommitSeq 严格递增且大于 Mapping CoveredCommitSeq；
6. 用户 Commit 原子应用整个 Mutation 集合；Relocation 按 Entry 执行 expected-old-VAddr CAS；
7. 两类 Seal 共享一个严格递增的 CommitSeq 序列，必须按该序列重放。

重放与在线提交共用 Mapping `PublishGroup`：用户 Put/Delete 和成功 Relocation 都使用 Mapping 中已有的
OldRef 精确扣减旧 Segment，并按 descriptor 携带的 `PhysicalSize` 增加 NewRef；Relocation CAS skip 不改变
统计。恢复完成后 Mapping 与 SegmentStats 位于同一 CommitSeq，不需要第二次扫描 Mapping Root。

没有对应 Seal 的完整 CommitPart/RelocationPart 是未提交垃圾，可以忽略并由 GC 回收；它不得改变 Mapping。已经存在完整有效 Seal 时，缺 Part、CRC 不匹配或引用不存在的 Record 才是 corruption，不能作为 Abort 静默跳过。Active 尾部 torn Frame 按第 13 节截断规则处理。

BatchAbort 只用于诊断，不覆盖已经 durable 的 CommitSeal。协议禁止同一 BatchID 同时存在有效 Abort 和 CommitSeal；发现时视为 corruption。

## 15. Status(BatchID)

运行时维护有界最近 Batch 状态表。第一版不提供在线历史 Descriptor 索引；更老已分配状态返回 `ErrStatusExpired`，离线工具可以扫描诊断。

第一版状态：

```text
Open
Committing
Committed(CommitSeq)
Aborted
CommitUnknown
```

`Status` 只保证当前进程内 Batch、最近保留的 Batch 和未解决 CommitUnknown。Committed 状态同时返回 CommitSeq，其余状态的 CommitSeq 为 0。超过状态保留边界返回 `ErrStatusExpired`，不能谎报 NotFound。发生 CommitUnknown 时 Store 阻止 Mapping Checkpoint/GC 越过该 Batch，直到重启恢复或状态被确认，因此未决结果不会在查询前被回收。

`StatusRetention` 是 runtime budget，默认 65,536 且不得小于 `MaxOpenBatches`。已解决状态按终态完成顺序逐出；CommitUnknown 钉住到重新 Open。相同预算也是 replay terminal hard limit：每个 Open Batch 预留一个 terminal slot，内部 Relocation 显式预留；达到 75% 异步请求 Checkpoint，达到 100% 时 `Begin` 等待。Checkpoint 在 log barrier 前捕获保守 terminal 计数，Manifest durable 后才释放被 cut 覆盖的 slot。这样 crash recovery 的 `Statuses`/重复终态集合不会随空 Batch 或 Abort 无限增长。

Recovery 在应用 replay 时对每个 terminal BatchID 保持精确集合；超过配置上限返回 `ErrStatusCapacity`，要求以更高 `StatusRetention` 打开并立即 Checkpoint，而不是 OOM 或放弃重复 Commit/Abort 检查。Descriptor Part 只允许同一 Batch 连续出现，并受持久化 `MaxBatchMutations` 约束，避免恶意交错 Part 构造无界 map。

重启后的判定使用 Manifest cut 元数据：

- ReplayStart 后存在有效 CommitSeal/RelocationSeal：按 Seal 返回结果；
- ReplayStart 后存在 BatchAbort：返回 Aborted；
- BatchID 位于 `OpenBatchIDsAtCut`：完整扫描后仍无 Seal，返回 Aborted；
- `BatchID >= IssuedBatchIDHighExclusiveAtCut` 且小于恢复前 durable reserved high：视为切点后可能发放；无 Seal 时返回 Aborted，并且整个未用 reserve 尾部都不会再分配；
- 更早且不在切点 Open 集合、近期状态索引也无记录：返回 `ErrStatusExpired`，不能根据 Seal 缺失猜测 Aborted。

未分配、为 0 或高于已持久化 BatchID high watermark 的 BatchID 返回 `ErrNotFound`。一个已经分配但超过状态保留边界的 BatchID 必须返回 `ErrStatusExpired`。BatchID 永不复用使状态过期不会造成 ABA。

## 16. Active Segment Rotation

Rotation 只能由 append sequencer执行：

```text
停止向当前 Active 分配新 Frame
-> 分配永不复用的 NewSegmentID
-> durable ROTATION journal: Prepared(old, new, base generation)
-> 写 SegmentSeal/Footer 并 fsync old file
-> journal: OldSealed
-> rename old .active -> .seg 并 fsync data directory
-> journal: OldRenamed
-> 创建 new .active Header，fsync file 和 data directory
-> journal: NewCreated
-> 通过 Manifest 安装串行器发布 old=Sealed、new=Active
-> journal: ManifestInstalled
-> 删除 ROTATION journal 并 fsync journal directory
-> 恢复 append
```

每个 journal Phase 只有幂等前进动作：重复 Seal 校验既有 Footer，重复 rename 接受目标已存在且身份匹配，重复 create 只接受空且 Header 完全匹配的新文件，重复 Manifest install 通过 generation/文件集合确认已完成。任何身份或内容冲突都返回 corruption，不能覆盖文件。

Open 在普通 Frame replay 前先恢复 ROTATION journal。没有 journal 时，Manifest 未引用的正式 Data 文件不能自动收编；只能报告 orphan 或由显式离线工具处理。

Open Batch 可以引用已 Sealed Segment 中的 PutRecord。GC 将最终、仍可发布的 Put 引用作为 pending root
复制；已越过提交顺序点的 Batch 由 Mapping CAS 搬迁，仍为 Open 的 Batch 在原地改写 `RecordRef`。因此
长 Batch 不再排除候选，也不要求 source 一直保留到 Commit/Abort。

## 17. Close 与崩溃差异

Close 尽力让状态整洁，但不创造额外正确性：

- Open Batch Abort；
- Committing Batch 等待结果；
- 当前写缓存 flush/fsync；
- Active Segment 可以保持 Active，不要求为 Close 强制 Seal；
- Manifest high watermark 更新；
- 释放目录锁。

Kill -9、机器掉电和 Close 前崩溃必须使用同一 Recovery 路径得到正确结果。

## 18. 必测失败时间线

- Put Header/Payload/Padding 任意字节后崩溃；
- CommitPart 之间崩溃；
- CommitSeal Header/Payload 后崩溃；
- group write 成功、fsync 前崩溃；
- fsync 成功、Publish 前崩溃；
- Publish 一半时崩溃；
- Publish 完成、reply 前崩溃；
- IDReserve write/fsync/publish 各阶段崩溃；
- Segment Footer write/fsync/rename/dir sync 各阶段崩溃；
- Checkpoint cut 前后并发 Commit；
- Open Batch 跨 Segment rotation 后 Commit/Abort；
- Context 在 Commit 各阶段取消。

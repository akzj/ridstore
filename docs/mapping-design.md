# Mapping 设计

状态：Development contract v1

## 1. 目标

Mapping 实现：

```text
uint64 ID -> latest Data VAddr | NotFound
```

它是 ridstore 的读取索引和运行时可见状态，但不是唯一持久化真相。它必须：

- 覆盖完整 uint64 ID；
- 不按历史 high watermark 分配；
- 不要求 Open 时全量加载；
- 内存占用受配置上限约束；
- 支持 hot point lookup；
- 支持 Batch 原子发布；
- 支持 Delete 后真正移除 Mapping；
- 支持 GC expected-old-VAddr CAS；
- 支持增量 Checkpoint；
- 崩溃后可以由 Root + Commit Log 恢复。

## 2. 总体结构

```text
                 Atomic Mapping State
                /                    \
Committed Delta Overlay       Persistent Radix Root
        │ hit                         │ miss
        └──────── Lookup ─────────────┘
                                      │
                              Bounded Node Cache
```

### 2.1 Delta Overlay

保存自最近 durable Mapping Checkpoint 之后的 committed 修改：

```text
ID -> {VAddr | Delete, CommitSeq}
```

特点：

- 只保存最终状态，不保存历史版本；
- 用户 Commit 发布后立即可查；
- Checkpoint 使用双 Overlay 切换；
- 大小达到阈值触发 Checkpoint/backpressure；
- 不能无限增长；
- 不持久化为独立随机写文件，持久化依据是 Data Log Commit Descriptor。

### 2.2 Persistent Radix

使用 9-bit stride、最多 8 层、512 fanout 的不可变 copy-on-write Radix：

```text
Level 7 ... Level 1 -> child MapAddr
Level 0              -> Data VAddr
```

八层覆盖完整 uint64；最高层只使用最高 1 bit。Node 只在存在有效子项时创建，空 Node 剪枝，因此容量与有效 Mapping 分布相关，不与历史最大 ID 线性绑定。

逻辑上每个 Node 有 512 个 Slot；磁盘上根据 occupancy 使用 SparseBitmap 或 Dense512。SparseBitmap 保存 512-bit occupancy bitmap 和紧密排列的非零 value，Dense512 保存完整 Slot 数组。两种编码不改变路径和 MapAddr 语义。

Persistent Radix 是 Mapping Checkpoint，不在每个用户 Commit 时写 Node。

## 3. 为什么不直接使用 LSM Mapping

Mapping 只处理 uint64 精确 Lookup，不需要任意 Key 排序、Range Scan 或 Level Compaction。第一版选择 Radix 的理由：

- ID 位分解直接决定路径；
- 上层节点可以长期缓存；
- Lookup 不需要 Key Comparator；
- Delete 后可以 COW 剪枝；
- Checkpoint 只重写受影响路径；
- 不引入另一个通用 LSM 引擎。

如果原型证明 Radix 无法满足写放大或冷读要求，替换必须保持 Mapping 接口和 Commit Log 格式，不得扩张 ridstore 的外部 Key 模型。

## 4. 内存状态

```go
type mappingState struct {
    root         MapAddr
    coveredSeq   CommitSeq
    statsBase    *segmentStatsBase // exact at coveredSeq
    frozen       []*deltaTable // newest first, waiting for checkpoint install
    activeDelta  *deltaTable
}
```

`mappingState` 通过 atomic pointer 发布。`statsBase` 与 Root 属于同一 checkpoint generation；每个 Delta Table 同时保存该层成功 NewVAddr 的 per-segment `upperAddBytes/Records`。因此运行时上界是 `statsBase + active additions + all frozen additions`。Checkpoint 安装只移除已经精确吸收到新 Stats Base 的 merged layers，cut 后 active/较新 frozen 的增量不会因“清零全局计数器”而丢失。另有独立的 `publishEpoch atomic.Uint64` 保护 active Delta 的原地全批发布；它不能放在 immutable `mappingState` 中。Node Cache、Delta Table、Root 和 Stats Base 都有独立生命周期引用。

Delta Entry：

```go
type deltaEntry struct {
    vaddr     VAddr // 0 means Delete
    commitSeq CommitSeq
}
```

Delta 的每个 ID shard 内同时保存一个按 SegmentID 聚合的 stats-addition map；mutation 与其 NewVAddr addition 在同一次 shard/publish 临界区更新，不引入按 SegmentID 的第二套锁顺序。读取全局上界时对所有 Delta shard/layer 求和，frozen 后可预聚合成 immutable 表。Stats additions 只增不减：用户 Put 和成功 Relocation 累加 NewVAddr 的完整 Frame bytes/count，Delete、失败 CAS 和 Abort 不累加。

第一版 Delta Table 使用分片 hash table；它不是磁盘格式，也不向外暴露。Active Delta 的每个 shard 有独立 RWMutex，普通 Go map 绝不能仅凭 epoch 做无锁并发读写。Frozen Delta 发布后不可变，不再需要 shard 写锁。分片数和 map 实现由基准决定。

## 5. Lookup

单次 Get Lookup：

```text
loop:
  epoch1 = publishEpoch.Load()
  if epoch1 is odd:
      retry
  state1 = atomic.LoadAndPin(mappingState)
  read activeDelta shard under shard RLock
  if miss, read immutable frozen deltas newest-to-oldest
  epoch2 = publishEpoch.Load()
  state2 = atomic.Load(mappingState)
  if state1 != state2 || epoch1 != epoch2 || epoch2 is odd:
      unpin state1
      retry
  if overlay hit:
      unpin state1
      return overlay result

  // The stable overlay miss above is the Get linearization point.
  // state1 pins the old Root even if a later Checkpoint installs a new Root.
  result = persistentRadix.Lookup(state1.root, ID)
  unpin state1
  return result
```

这样 Checkpoint Root/Delta 切换不会返回两代状态混合结果。epoch 只覆盖内存 Overlay 检查，不跨越冷 Radix I/O；Overlay miss 验证成功后，即使随后有 Commit，返回旧 Root 的结果仍线性化在该 Commit 之前。

Batch Publish 在 activeDelta 内必须全批可见。第一版采用短写锁：

```text
lock publishMu
publishEpoch++ // odd
lock affected activeDelta shards in ascending shard order
apply all mutations
unlock affected shards in reverse order
publishEpoch++ // even
unlock
```

Lookup 读取 `publishEpoch`，若为 odd 或前后变化则重试。Shard lock 只保护 Go 容器的内存安全，epoch 负责跨 shard 的 Batch 可见性。磁盘 Radix I/O 不持有 publishMu 或 shard lock。

单次 Get 在并发 Commit 前后可以得到旧值或新值；不会看到同一 Batch 的内部部分状态。多个独立 Get 不自动形成 Snapshot。

## 6. Persistent Radix Lookup

给定 ID：

1. 从 Root MapAddr 获取 Level 7 Node；
2. 按对应 9-bit stride 选择 Slot；
3. Internal Slot=0 返回 NotFound；
4. 否则加载 child Node；
5. 到 Level 0 后 Slot 保存 Data VAddr；
6. 校验 Node Level、Prefix、CRC 和 Store UUID/File identity。

Node Cache key 为 MapAddr。缓存项不可变，因此不同 Root 可以安全共享旧 Node。

冷 Lookup 最坏可能读取 8 个 Node。实际运行中 Root 和高层 Node 必须常驻 Cache；常见冷读目标是 1 个 Leaf I/O 加 Data Record I/O。

路径计算固定为：

```text
level 0 slot = (ID >> 0)  & 0x1ff
level 1 slot = (ID >> 9)  & 0x1ff
...
level 6 slot = (ID >> 54) & 0x1ff
level 7 slot = (ID >> 63) & 0x001
node prefix  = level == 7 ? 0 : ID >> (9 * (level + 1))
```

Level 7 的 Slot 2..511 必须为 0。Node Header 的 Prefix 保存到达该 Node 之前已经消费的高位值；加载 child 时必须由 parent prefix、slot 和 child level 重新计算并校验，不能只相信磁盘指针。

读取 SparseBitmap 时先检查目标 bit，再对之前的 bitmap word 执行 popcount 得到 packed value index。读取 Dense512 时直接下标访问。空 Node 不落盘；删除最后一个 Entry 后父 Slot 变为 0。

## 7. Node Cache

Node Cache 使用字节容量而不是条目数量限制。要求：

- Root 和配置指定的上层 Node pinned；
- 其余 Node 使用 CLOCK/SLRU 类淘汰策略，具体算法由基准决定；
- 正在 Lookup 的 Node 有引用计数，不能被释放；
- Cache miss 合并同一 MapAddr 的并发加载；
- SparseBitmap 在 Cache 中保持 bitmap+packed values，不为了查询方便无条件展开成 4096-byte Dense 数组；
- CRC 失败不负缓存，Store fail closed；
- Cache 统计 hit/miss/load latency/eviction/pinned bytes；
- Cache 容量同时计算编码数据和 Go 对象/索引开销，MappingCacheBytes 达到上限时仍可通过淘汰继续工作。

第一版不依赖操作系统无限 Page Cache；基准同时报告 Library Cache 和进程 RSS。

## 8. Batch Publish

输入是已经 durable、同一 CommitSeq 的最终 Mutation 集合。

用户 Batch 的 ExpectRevision/ExpectAbsent 已在生成 Seal 前由 Commit Coordinator 验证；Mapping Publisher 不重新验证条件，只发布已经 durable 的 admitted Batch。Recovery 同样只重放 Seal。LogicalRevision 是 PutRecord Header 的 OriginBatchID，不进入 Radix Slot；Relocation 更新 VAddr 时保留 OriginBatchID，因此不改变 Revision。

规则：

- 按 CommitSeq 严格递增发布；
- Put 设置 ID->VAddr；
- Delete 设置 Delta tombstone，覆盖 Persistent Root 中的旧值；
- 同一 ID 的较大 CommitSeq 覆盖较小 CommitSeq；
- Publish 完成前不回复 Commit 成功；
- Publish 不执行文件 I/O；
- 运行时 tombstone 只存在到下一次包含它的 Checkpoint。

Checkpoint 把 Delete 合并进 Radix 后，对应 Leaf Slot 变为 0；Delta tombstone 随 captured overlay 释放。

## 9. GC CAS

GC Relocation Entry：

```text
ID, ExpectedOldVAddr, NewVAddr
```

GC Relocation 的 CAS 在 Commit Coordinator 的 pre-Seal virtual Mapping 阶段解析：

```text
current = Mapping.Lookup(ID)
if current == ExpectedOldVAddr:
    resolved outcome = apply NewVAddr
else:
    resolved outcome = skip
```

该 Lookup 可以发生冷 Root I/O，但不持 `publishMu`；Coordinator 在验证到该 group 完成 Publish 期间不允许后续 group 或 Checkpoint barrier 越过，并把本 group 之前已通过的 mutation 保存在 virtual Mapping 中。Relocation Descriptor 始终保存原始 ExpectedOldVAddr/NewVAddr，不把运行时 outcome 写入磁盘。

fsync 后 Mapping Publisher 按 coordinator 给出的 immutable resolved plan 执行纯内存 apply/skip，不再次调用可能 I/O 的 Lookup。成功结果与 Relocation CommitSeq 一起进入 Delta，失败的 NewVAddr 没有 Mapping 指向并成为垃圾。若 Publisher 观察到内存状态与 resolved plan 前提不符，表示提交串行化实现错误，Store 必须 fail closed。

恢复没有并发 Publisher，按 CommitSeq 从 Root+replay state 执行原始 Descriptor 的同样 CAS，必须得到与运行时 resolved plan 相同的结果，不能无条件覆盖。

## 10. Checkpoint 双 Overlay

触发条件：

- Delta bytes 达到阈值；
- Commit count/Log bytes 达到阈值；
- 显式 `Checkpoint`；
- Close 可触发，但正确性不依赖 Close；
- GC 需要推进安全删除边界。

同一时刻只运行一个 Mapping Checkpoint。Checkpoint 向 append/commit coordinator 提交 barrier 请求；coordinator 在 Commit publish 序列轮到该请求时进入以下临界阶段：

```text
publishMu.Lock
C = latest fully published CommitSeq
F = end of the confirmed-durable contiguous Data Log prefix
ReplayStart = exact position after F
captured = activeDelta
activeDelta = new empty delta
baseRoot = current root
frozen.prepend(captured)
publish new mappingState(baseRoot, current statsBase, frozen, activeDelta)
merged = snapshot of all frozen layers, oldest-to-newest
publishMu.Unlock
```

`(C, F, ReplayStart)` 的选取和 Delta 切换不可分离。更晚 Commit 只有在 `publishMu` 释放后才能发布到新 active Delta，因此 merged 中不会混入 `CommitSeq > C`。F 不能越过只 write 尚未 fsync 的 Active 尾部。

新的 Commit 继续进入 activeDelta，不等待磁盘 Checkpoint。Lookup 顺序为 activeDelta、frozen newest-to-oldest、persistent root，因此 durable Root 安装前 captured 仍然可见。

后台将 `merged` 中所有 frozen layer 按从旧到新的顺序折叠，每个 ID 只保留最新 mutation；再按 ID 排序、按 Leaf 分组，基于 `baseRoot` 构建新的 COW Radix Root。Builder 同时从 base Root 的 OldVAddr 与 cut 时 final NewVAddr 生成精确 `SegmentStats(C)`；Header 读取按 SegmentID/offset 排序并在所有 `publishMu`、Delta shard lock 之外执行。必须包含以前失败 Checkpoint 留下的 frozen layer，不能只合并本轮 captured。完成后：

```text
fsync mapping files
publish Manifest(newRoot, CoveredCommitSeq=C, CutFrameSeq=F, ReplayStart,
                 StatsCoveredCommitSeq=C, SegmentStats(C))
publishMu.Lock
verify merged is still the complete frozen set selected by this checkpoint
publish new immutable runtime state(newRoot, coveredSeq=C,
                                    exactStats(C), frozen without merged,
                                    current activeDelta)
publishMu.Unlock
release merged overlays after readers unpin
```

Lookup 在安装期间始终通过 `active + frozen + root` 得到正确结果；不能在 Root durable 前丢弃任何 merged layer。Root/frozen/Stats 的内存切换也必须发布同代 immutable state，不能分别修改字段。Checkpoint 构建、Header 校验、Stats underflow、mapping fsync 或 Manifest 安装任一失败时，旧 Root/旧 exact Stats 与所有 frozen layer 原样保留；下一次必须连同新 captured 一起合并。不得安装新 Root 后再异步补同一 cut 的 Stats。

## 11. COW Radix 构建

为避免每个 ID 重写八个 Node：

1. merged entries 按 ID 排序；
2. 同一 Leaf 的修改合并；
3. 从旧 Leaf 按需读取并应用全部 slot 变化；
4. Leaf 非空则按 occupancy 选择 SparseBitmap/Dense512 并 append，空则返回 MapAddr 0；
5. 将 child 变化按父 Prefix 分组；
6. 自底向上每个受影响 Internal Node 只重写一次，并重新选择编码；
7. 最终得到 NewRoot；
8. 所有 Node append 后 fsync Mapping Segment；
9. Manifest 原子安装 Root。

新 Root 可以引用未修改的旧代 immutable 子树。Node Header 的 `CoveredCommitSeq` 表示该 Node 自身最后一次重写的 cut，因此 Root 和可达子 Node 只要求不晚于 Manifest cut；空 Commit 后的 Checkpoint 可以原样复用 Root。若强制所有可达 Node 与新 Root 同代，就会退化为每次全树重写。

Checkpoint 的写放大同时按“受影响 Node 数”和编码后 bytes 衡量。必须记录：dirty IDs、dirty leaves、rewritten internal nodes、Sparse/Dense Node 数、occupancy histogram、bytes written、dense-equivalent bytes 和节省比例。

Builder 按 `CheckpointMemoryBytes` 把 frozen layer 切成有界 ID chunk，逐 chunk 在上一轮新 Root 上继续 COW；不会再 materialize 全量 Mapping。精确 SegmentStats 通过顺序遍历新 Root、逐条校验 Data Header 构建。第一版 SegmentStats 聚合表若超过预算会明确拒绝本次 Checkpoint 并保留 frozen layers，后续可替换为外部 run merge，不能退回无界内存。

## 12. Checkpoint 失败

- 构建/写入失败：旧 Root + frozen/active Overlay 继续提供服务；
- fsync 不确定：Store 停止新写，重启根据 Manifest 判定；
- Manifest 发布前崩溃：旧 Root 仍有效，新 Mapping Nodes 为孤儿；
- Manifest 发布后崩溃：新 Root 有效，恢复从新 ReplayStart 开始；
- merged frozen layers 只有在确认新 Manifest durable 后才能释放；
- Checkpoint 错误必须可观测，不能像成功一样推进 Log/GC 删除边界。
- SegmentStats 构建失败与 Root 构建失败具有相同语义：整个 checkpoint generation 不安装；

MappingCheckpoint 的 Maintenance Journal phase 固定为：Prepared、NodesWritten、MappingFilesDurable、ManifestInstalled、Complete。ManifestInstalled 之前恢复使用旧 Root，并保留或清理不可达新 Node；之后恢复必须使用新 Manifest。Journal 只证明文件安装进度，内存 Mapping State 始终可由 Manifest + replay 重建。

## 13. Mapping 文件 GC

Mapping Segment rotation 只由 checkpoint/Mapping GC writer 执行，并作为当前 Maintenance Journal operation 的子阶段记录：seal/footer fsync、rename+directory fsync、new active header fsync、Manifest file-set install。MapAddr 只按 FileID 解析，`.active`/`.seg` 后缀不改变已有 Root 的可读性；Journal 未完成时 Open 必须先完成或回滚 rotation，不能猜测收编孤儿文件。

COW 会产生不可达旧 Node。Mapping GC 独立于 Data GC：

1. pin 当前 Root；
2. 遍历可达 Node；
3. 将可达 Node copy 到新 Mapping generation，重写 child MapAddr，并按当前 occupancy 重新选择 SparseBitmap/Dense512；
4. fsync 新 Mapping files；
5. 安装指向新 Root 的 Manifest；
6. 等待旧 Root/Node Cache 引用释放；
7. 将旧 Mapping files 移入 trash 并 fsync 目录；
8. 删除 trash 文件。

Mapping GC 不改变 ID->Data VAddr 内容。发生错误时保留旧 Root；不能删除旧文件后再发布新 Root。

Mapping GC 与普通 Mapping Checkpoint 不能并发构建并互相安装过期 Root。第一版由 checkpoint coordinator 串行执行两者；最终 Manifest 仍通过全局安装串行器和 generation CAS 与 Data GC、Segment rotation 合并。

MappingGC 的 Maintenance Journal phase 固定为：Prepared、Copied、MappingFilesDurable、ManifestInstalled、OldFilesTrashed、Deleted。ManifestInstalled 前旧文件不能移动；安装后先等待旧 Root/Cache pin 清零，再进入 trash/delete。

第一版可以暂缓自动 Mapping GC，但必须暴露 unreachable mapping bytes，且长期 soak 前必须实现空间收敛。

## 14. 内存与磁盘下界

精确 Mapping 对 N 个独立有效 ID 至少需要 O(N) 位置信息。ridstore 不承诺 Mapping 与有效 ID 数无关。

目标是：

- 磁盘保存完整 Mapping；
- 内存只保存 Delta 和有界 Cache；
- 删除后的 ID 在 Checkpoint 后不保留永久 tombstone；
- 历史 high watermark 不导致同等大小的空数组；
- 冷 Lookup 用磁盘 I/O 换内存容量。

## 15. Backpressure

Checkpoint 速度若追不上 Commit，Delta 会增长。达到软/硬限制：

- soft limit：提高 Checkpoint 优先级并记录告警；
- hard limit 统计 active Delta、全部 frozen Delta 和已 admission 未 Publish 的 reservation；
- 可能越过 hard limit 的 Commit 在 CommitSeq/Seal 之前等待预算；已经 durable 的 Commit Publish 永不因限额阻塞；
- 不允许 OOM 后由进程被杀作为流控；
- 不允许丢弃 Delta，因为它承载最新可见 Mapping；
- Context 在预算等待阶段取消是确定未提交；只有 Seal 已进入不可确定持久化阶段才适用 CommitUnknown。

精确计费、默认值和 reservation 生命周期见 `runtime-config.md`。

## 16. 第一版实现顺序

1. 先实现 `memoryMapping` 作为参考模型和 Commit/Recovery 验证；
2. 定义统一 Mapping 接口和模型测试；
3. 实现 Persistent Radix codec/golden vectors；
4. 实现 Node Cache；
5. 实现 Delta + Root Lookup；
6. 实现双 Overlay Checkpoint；
7. 实现 crash tests；
8. 最后切换默认 Mapping；
9. memoryMapping 保留在测试中作为 oracle，不作为生产默认。

不得先让业务代码依赖 memoryMapping 的全量加载行为。

## 17. 验收指标

- 完整 uint64 高位 ID 正确 Lookup；
- 稀疏 ID 不按 high watermark 分配；
- Open 只加载 Root/必要上层 Node；
- Mapping Cache 严格受预算限制；
- Delta hard limit 能 backpressure；
- Delete Checkpoint 后 Leaf/路径可剪枝；
- 稀疏随机 ID 不写入整页空 Slot，Delete 后 Node 可以 Dense→Sparse 或被剪枝；
- 并发 Publish/Lookup 无 partial batch；
- Checkpoint 与并发 Commit 不丢更新；
- Manifest 任意崩溃点只选择旧 Root 或新 Root；
- GC CAS 在运行和恢复中结果一致；
- 长时间运行 Mapping 文件空间最终收敛。

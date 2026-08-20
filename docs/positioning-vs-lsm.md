# ridstore 与 LSM/RocksDB 的定位边界

状态：Accepted project boundary

目的：防止研发过程中将 ridstore 漂移成 RocksDB/LevelDB 的替代品

## 1. 决策

ridstore 不是通用有序 KV 引擎，也不以替代 RocksDB、LevelDB、Pebble 等 LSM 引擎为目标。

ridstore 专注于更窄的模型：

```text
uint64 stable ID -> variable-length bytes
```

它通过稳定逻辑 ID、物理地址 Mapping、append-only Record 和原子 Batch，为已经拥有上层索引或只需要精确 ID 寻址的系统提供底层 Record Store。

LSM 与 ridstore 都使用顺序写和后台空间整理。ridstore 的差异化价值不是“也采用 append-only”，而是主动放弃任意 Key 的有序能力，从而不为不需要的排序、Sorted Run 和多层 Compaction 付费。

## 2. 两种架构解决的问题不同

### 2.1 LSM

LSM 解决：

```text
arbitrary ordered key -> value
```

典型写入路径：

```text
WAL
-> MemTable
-> L0 SSTable
-> L1/L2/... Compaction
```

排序带来的能力：

- 任意字节 Key；
- Point Lookup；
- Range Scan；
- Prefix Scan；
- 有序迭代；
- Bloom Filter 和 Block Index；
- 通过 Sorted Run 合并删除和旧版本。

其主要代价是 MemTable/SSTable 维护、Key 比较和多层 Compaction。

### 2.2 ridstore

ridstore 解决：

```text
stable uint64 ID -> latest physical Record
```

典型写入路径：

```text
append Record
-> append Batch Commit Marker
-> group fsync
-> publish ID -> VAddr Mapping
```

它提供：

- 稳定且永不复用的 ID；
- 精确 ID Lookup；
- Record 更新和删除；
- Batch 原子提交；
- 崩溃恢复；
- Mapping Checkpoint；
- Segment GC。

它主动不提供 Key 顺序。其主要代价是 Mapping、Mapping Cache、Checkpoint 和 Segment Cleaning。

## 3. 根本交换

两个系统都没有消灭后台整理，只是选择了不同的成本模型：

```text
LSM
= Sorted Index
+ MemTable Flush
+ Multi-level Compaction

ridstore
= ID Mapping
+ Mapping Checkpoint
+ Segment Cleaning
```

真正需要验证的问题是：

> 在稳定 ID、Point Read、高覆盖/删除率的工作负载中，Mapping Lookup 与 Segment Cleaning 的总代价，是否小于排序、Flush 和多层 Compaction？

答案只能由目标工作负载下的稳定态测试给出，不能由“append-only 更快”直接推出。

## 4. ridstore 可能具有的优势

### 4.1 不维护无用的 Key 顺序

如果上层已经拥有 PageID、BlobID、ObjectID 或自己的 B-Tree，那么底层通常只需要：

```text
Get(ID)
Put(ID, Value)
Delete(ID)
```

将这种对象再存入 RocksDB，会额外维护 Key 比较、MemTable、Sorted SSTable、Block Index、Bloom Filter 和 Level Compaction。ridstore 可以省略这些有序 KV 能力。

典型例子是：

```text
PageID -> serialized B-Tree node
```

上层 B-Tree 已经负责排序；底层再次按 PageID 构建 LSM 排序结构可能是重复成本。

### 4.2 Record 同时是数据和恢复来源

ridstore 的目标是让 append 的 Record 本身成为最终物理数据，并通过同一 Log 中的 Commit Marker 提供恢复依据：

```text
append Record
-> durable Commit Marker
-> Mapping points to that Record
```

它不需要先写完整 Value 到独立 WAL，再把同一 Value Flush 到 SSTable。Mapping Checkpoint 只保存紧凑位置元数据。

这项优势只有在 Record、Commit Marker 和 Mapping 的持久化顺序正确时才成立。metadata-only WAL 指向未持久化 payload 不属于 ridstore 的合法实现。

### 4.3 Point Read 路径更直接

理想热路径：

```text
ID
-> Mapping Cache
-> VAddr
-> immutable Record
```

LSM Point Read 通常需要依次考虑 MemTable、Immutable MemTable、L0 和后续 Level 的 Bloom/Index/Data Block。现代 RocksDB 已对该路径高度优化，因此 ridstore 不能只凭步骤更少就宣称一定更快；优势必须通过热 Mapping 和冷 Mapping 两类测试证明。

### 4.4 避免大 Value 的多层 Compaction

LSM 中的 Value 可能随 Flush 和 Level Compaction 被多次搬迁。ridstore 更新大 Value 时只 append 新 Record 并切换 Mapping，旧 Record 等待合适的 Segment GC 时机。

因此，中大型 Value、高覆盖率和高删除率越明显，ridstore 越可能降低完整 Value 的重复写入。

### 4.5 按真实死亡率选择回收对象

ridstore 可以优先清理 dead ratio 高的 Segment，只复制其中仍被 Mapping 引用的 Record。

若候选 Segment 的 live ratio 为 `u`，经典清理成本近似为：

```text
steady-state cleaning write amplification ~= 1 / (1 - u)
```

示例：

| Segment live ratio | 近似写放大 |
|---:|---:|
| 20% | 1.25x |
| 50% | 2x |
| 80% | 5x |
| 95% | 20x |

这说明 GC 优势不是无条件的。低 live ratio 很有利；大部分 Record 长期存活时，Segment Cleaning 同样会产生严重写放大。

## 5. LSM 明确占优的场景

出现以下核心需求时，应优先选择成熟 LSM，而不是扩张 ridstore：

- 任意字节 Key；
- Range Scan、Prefix Scan 和有序 Iterator；
- 需要底层直接维护 Key 顺序；
- 数据按 Key 排序后具有显著压缩收益；
- 大量写入一次、长期存活的小 Value；
- 上层没有自己的索引结构；
- 需要成熟的 Snapshot、Column Family、Backup、压缩和生态工具；
- 团队无法承担自研存储的恢复、校验、升级和长期运维成本。

如果上层最终在 ridstore 上重新实现完整有序 KV，需要计算：

```text
上层索引写放大
+ ridstore Mapping 成本
+ Segment GC 成本
```

这个组合可能比直接使用 RocksDB 更复杂、更慢，也更难验证。

## 6. ridstore 的目标工作负载

ridstore 优先验证以下通用存储特征，而不是绑定某项具体业务：

- 标识已经是稳定 uint64 ID；
- 读取主要是精确 Point Read；
- 上层已经有自己的索引或对象关系；
- 同一逻辑 ID 可能被反复覆盖或删除；
- Value 为中等或较大尺寸；
- 多个修改需要 Batch 原子提交；
- 高并发写入可以通过 group commit 摊销 fsync；
- 使用者愿意用上层结构换取更窄、更直接的底层存储路径。

这些是性能假设，不是已证明结论。

## 7. ridstore 不承诺的优势

项目不能在没有证据时声称：

- 所有写入都比 RocksDB 快；
- 所有 Point Read 都更快；
- append-only 天然等于 crash-safe；
- GC 一定比 Compaction 成本低；
- Mapping 可以不随有效 ID 数量增长；
- Batch group commit 是 ridstore 独有能力；
- 初始空库基准可以代表长期稳定性能；
- 关闭 fsync 后的吞吐可以与 durable RocksDB 公平比较。

## 8. 公平验证方法

ridstore 的研发价值必须在与 RocksDB/LevelDB/Pebble 相同 durability 条件下验证。

### 8.1 工作负载矩阵

至少覆盖：

- Value：128B、4KiB、1MiB；
- 操作：create-only、overwrite-heavy、delete-heavy、mixed read/write；
- Batch：1、10、100、1000；
- 并发：单写者与多写者；
- 数据集：小于内存和显著大于内存；
- Mapping：热命中、冷命中和受限 Cache；
- 空间状态：不同 Segment live ratio；
- 运行阶段：初始加载、进入 GC/Compaction、长期稳定态；
- 恢复：短 Log、长 Log、Checkpoint 后、维护中崩溃。

### 8.2 必须报告的指标

- Commit p50、p99、p999；
- queue wait、write、fsync 和 publish 分段延迟；
- 用户写入字节与实际磁盘写入字节；
- GC/Compaction 搬迁字节；
- CPU、RSS、Mapping/Block Cache 使用量；
- Point Read 热/冷延迟和实际 IOPS；
- 磁盘空间放大；
- 崩溃恢复时间；
- 前台延迟在 GC/Compaction 期间的变化；
- 维护结束后资源和文件数量是否收敛。

### 8.3 公平性约束

- durability 必须相同；
- fsync 策略必须明确；
- 压缩应关闭后对比一次，再分别报告开启压缩的实际结果；
- Cache 预算必须接近；
- 数据集必须超过 Cache 才能评价冷读；
- 必须运行到 GC/Compaction 稳定态；
- 不能只比较平均吞吐，必须比较尾延迟和写放大；
- 环境、文件系统、磁盘和参数必须完整记录。

## 9. 防漂移规则

以下变化属于项目定位变更，不能作为普通功能直接加入：

- 将外部 Key 从 uint64 ID 扩展为任意 `[]byte`；
- 在内核增加 Key Comparator；
- 在内核增加 Range/Prefix Scan；
- 引入以 Key 排序为目标的 SSTable；
- 引入 Level Compaction 作为主要数据组织方式；
- 让 Page、Blob、Stream、SQL 或 TTL 成为核心 Record 语义；
- 为追求 RocksDB API 兼容而扩大 Store 接口。

任何此类提案必须先回答：

1. 为什么上层数据结构不能解决？
2. 为什么不能直接使用成熟 LSM？
3. 是否破坏稳定 ID Record Store 的窄边界？
4. 是否引入了排序和 Mapping 两套索引成本？
5. 是否有稳定态基准证明总体复杂度值得？

如果答案不能证明必要性，应拒绝进入 ridstore 内核。

## 10. 成功标准

ridstore 的成功不是功能覆盖 RocksDB，而是在明确边界内证明：

```text
对于 stable-ID + Point Read + overwrite/delete-heavy 工作负载，
它能以更低或更可预测的写放大、尾延迟和恢复复杂度，
提供正确的 durable atomic batch。
```

如果稳定态测试不能证明该优势，项目应保留为专用 Record Store 或停止扩张，而不是通过增加 LSM 功能寻找新的存在理由。

# ridstore v2 Bounded Checkpoint Builder

状态：Implementation contract

## 1. 问题边界

Delta admission 已限制 `active + frozen + reserved` 的 entry 总量，但旧 Checkpoint 路径仍会同时创建：

```text
latest map[ID]value
sorted []Mutation
每一级 []childChange
```

这些结构都随 Delta 大小增长，且 Go map 的实际开销难以从配置准确推导。仅检查 entry 数不能证明
Checkpoint 的额外工作集有界。

本模块只解决 Mapping COW build 的临时内存。Delta 本体由 Delta hard limit 约束，Radix cache 由
MappingCacheBytes 约束，SegmentStats 由独立的 MaxSegmentStats 约束；三者不能共享一个含义模糊的配置。
进程内 `RecordMetaCacheEntries` 只是 SegmentStats 的随机读加速层，miss 仍读取并验证
Record/Put header，它不改变 Builder 或 SegmentStats 的正确性边界。

## 2. 选择

当前 Delta 已有硬上界，因此不引入磁盘临时 run。外部排序会额外产生 scratch 文件命名、fsync、崩溃清理
和目录所有权协议，而这些文件对恢复没有价值。

Checkpoint 使用一块预先计费的 `[]radix.Mutation`：

1. 按 frozen layer 从旧到新追加 mutation；
2. 对 ID 做原地稳定排序；相同 ID 保持 layer 顺序；
3. 原地压缩重复 ID，只保留最后一个 mutation；
4. 把严格递增的 slice 交给 `Tree.BuildSorted`；
5. BuildSorted 逐 leaf 消费，并通过固定 8 层 accumulator 向 Root 传播 child change。

因此可变 scratch 上界是：

```text
frozen layer entries * 16 bytes
```

另外只有与树深度相关的固定 accumulator 空间，不再存在 `latest map`、第二份 mutation copy 或每层
O(N) child-change slice。

## 3. 配置

```text
CheckpointSortBytes  // 只约束 mutation 排序数组
MaxSegmentStats      // 只约束输出的 sealed-segment stats 条目数
```

配置必须满足：

```text
floor(DeltaHardLimitBytes / 64) <= floor(CheckpointSortBytes / 16)
```

这保证 admission 能形成的最坏 frozen entry 数一定能进入排序数组。固定 radix accumulator、Delta 本体、
cache 和 stats 各有独立边界，不能把 CheckpointSortBytes 描述为整个进程的内存上限。

## 4. 原子性与失败

- 全部输入顺序、ID 和 VAddr 在写第一个新 Node 前完成验证；
- Node append 中途失败只留下不可达 COW Node，不改变 runtime Root 或 Catalog；
- BuildSorted 返回的新 Root 只有在 MapStore Sync 和 Catalog checkpoint tuple durable 后才能安装；
- stable sort 是正确性条件：同一 ID 横跨多个 frozen layer 时必须保留最新 layer 的值；
- empty/no-op build 只推进 covered sequence，不制造无意义 Node。

## 5. 不做的事情

- 不创建 checkpoint scratch 文件或第二套文件生命周期；
- 不改变 immutable Radix Node 的磁盘格式；
- 不把 SegmentStats 塞入 Mapping builder；
- 不把 soft-limit 后台调度描述为 Builder 的内部职责；调度属于 Engine，
  Builder 仍只消费一个已 freeze 的有界输入；
- 不把固定 entry charge 描述成 Go heap 的精确测量。

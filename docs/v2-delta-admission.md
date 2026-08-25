# ridstore v2 Delta Admission

状态：Implementation contract

## 1. 为什么它是当前架构门禁

Persistent Mapping 的运行时状态是：

```text
persistent radix Root + frozen Delta layers + active Delta
```

没有 admission 时，durable Commit 可以无限增加 Delta。结果不是单纯的性能下降，而是两个正确性问题：

- 进程可能在下一次 Checkpoint 前耗尽内存；
- frozen layers 可能超过 Builder 预算，使 Checkpoint 永久无法释放 Delta。

因此 Delta hard limit 必须在 Relocation/GC 和公开 API 切换前完成。GC 本身也是 Delta 写入者，不能先把
它接到一个无界 Mapping 上。

## 2. 唯一所有者

Persistent Mapping 同时拥有：

- active/frozen layer 的实际内存；
- charged/reserved Delta 预算；
- Checkpoint 安装后释放 frozen charge 的权力。

Coordinator 只持有 reservation，并在 Descriptor durable 前释放或在 Publish 时消费。Engine 只负责在
hard pressure 时推进 Checkpoint，不能直接修改预算数字。

内存 Mapping 只作为模型 oracle，可以使用无限预算；生产 Engine 只接受 Persistent Mapping。

## 3. 计费模型

第一版使用保守的固定 entry charge：

```text
DeltaEntryCharge = 64 bytes

charged  = active layer charge + all frozen layer charge
reserved = admitted but not yet published upper bounds
used     = charged + reserved
```

Put、Delete 和成功 Relocation 都可能在 active layer 创建一个 entry。对 active layer 已存在的同一 ID
再次修改不增加实际 charge。同一 group 按提交顺序计算，只有首次创建该 active entry 的 proposal 消费
charge；冲突 proposal 和 Relocation CAS skip 不消费。

固定 charge 是安全预算而不是 Go heap 的精确测量。修改 entry 表示、hash table 或对象布局时必须重新
基准并提高 charge；不能为了提高 benchmark 数字低估内存。

## 4. Reservation 时序

每个 Commit 在进入 Coordinator queue 前，按最终 mutation 数量预留最坏上界：

```text
upper = mutation count * DeltaEntryCharge
```

规则：

- 单请求 upper 大于 hard limit，确定返回 `ErrBatchTooLarge`；
- `used + upper <= hard` 时原子增加 reserved；
- 达到 soft limit 返回 pressure signal；当前迭代只记录该信号语义，尚未启动后台主动 Checkpoint；
- hard limit 暂时不足时不 Prepare Batch、不写 Descriptor、不改变 Mapping；
- Context 取消、Prepare 失败、条件冲突、编码失败或 pre-durable append 失败必须释放 reservation；
- durable success 后 Publish 在同一 Mapping 临界区校验 plan、消费实际 charge 并释放 reservation 余量；
- 一旦 Descriptor 允许 durable，Publish 不能再因预算不足失败。

Recovery 同样受 hard limit：它为 replay group 做立即 reservation。若 checkpoint 之后的合法 durable tail
无法放入本次 Open 配置的预算，Open 返回明确配置错误，不能等待一个尚未运行的 Checkpoint，也不能 OOM。

## 5. Pressure 与 Checkpoint 无死锁时序

当前 Store 的 Checkpoint cut 使用 `ops.Lock`，普通 Batch 方法使用 `ops.RLock`。Commit 若持有 RLock 等待
Delta 空间，会形成：

```text
Commit waits Delta -> Checkpoint waits ops.Lock -> Delta waits Checkpoint
```

v2 禁止该锁环。Commit 的正确流程是：

```text
1. 获取 ops.RLock 并检查 Store lifecycle
2. 在同一个 ops.RLock 区间内 reservation、Prepare 并进入 Coordinator queue
3. queue 接管请求后立即释放 ops.RLock；不等待 durable result
4. hard pressure 时先释放 ops.RLock，由该调用者推进一次 Checkpoint
5. Checkpoint 安装释放 frozen charge
6. 重试 admission；成功后再等待 durable Publish
```

步骤 1–3 的 RLock 不能进一步缩短：否则 reservation 成功后、queue admission 之前可能被 Freeze 穿过，
reservation 针对旧 active layer 计算，而 Commit 最终写入新 active layer。RLock 只覆盖无 I/O 的 admission；
durable append 和调用者等待都不持有它。

Batch 在步骤 2–4 仍是 Open，因此 Checkpoint 可以把它记入 open-batch cut；它后续的 Commit Record 位于
该 cut 之后。Close 与 Commit 通过 Batch 状态机、Coordinator queue ownership 和 Close drain 协调，不能
依赖 Commit 长时间持有 Store lifecycle RLock。

多个 pressure caller 可以同时请求 Checkpoint；`checkpointMu` 保证实际构建串行。当前实现允许已经进入
等待队列的 caller 在前一轮释放空间后再构建一代空 checkpoint；这是吞吐优化缺口，不破坏容量或持久性
不变量，后续可用 pressure generation 去重。

## 6. Checkpoint 释放

Freeze 只把 active layer 移入 frozen，不释放 charge。Build/Manifest 失败也不释放。

只有以下顺序全部完成后才释放被安装 frozen prefix 的 charge：

```text
candidate Mapping nodes durable
-> Manifest checkpoint tuple durable
-> runtime installs candidate Root
-> remove exact frozen prefix
-> release that prefix charge
```

新 active layer 和未被本 candidate 覆盖的 frozen suffix 不释放。释放依据 layer 自己记录的 charge，不能
从新 Root 大小或 SegmentStats 反推。

## 7. 与 Builder budget 的关系

DeltaHardLimit 约束共享 active/frozen/reserved 状态；CheckpointSortBytes 约束 Builder 的 mutation 排序
数组。两者不能互相替代。Builder 使用单一原地排序数组与固定深度的 Radix accumulator，不再分配
`latest map`、第二份 mutation copy 或每层 O(N) child-change slice。

当前有界排序模型要求配置满足：

```text
floor(DeltaHardLimitBytes / DeltaEntryCharge)
    <= floor(CheckpointSortBytes / 16)
```

否则 Commit 可以合法填满 Delta，却没有任何一次 Checkpoint 能接纳它，hard admission 反而会形成永久
压力。Open/Create 会在创建文件前拒绝这种配置。

CheckpointSortBytes 只描述可变排序数组；Delta、Radix cache、固定 8 层 accumulator 和 SegmentStats
分别计费，不能把它解释成整个 Checkpoint 或进程的总内存上限。

## 8. 验收不变量

- `charged + reserved` 永不超过 hard limit；
- reservation 取消、冲突和任何 pre-durable 错误均归还；
- Publish 的实际 charge 不超过 reservation upper；
- hot ID 重复更新不会每次永久增加 active charge；
- Freeze/Abort 不释放，Install 只释放精确 frozen prefix；
- hard pressure 可以推进 Checkpoint，不与 `ops.Lock` 形成等待环；
- Checkpoint 连续失败时 Commit 停止在 durable 边界之前，Get/Abort/Close 仍可执行；
- replay 超预算确定失败，不等待、不部分发布；
- race test 覆盖 Commit、Checkpoint、Close 和 reservation 取消。

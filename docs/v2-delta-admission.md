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
- 达到 soft limit 返回 pressure signal；Commit admission 成功并释放 Coordinator admission read lock 后，
  Engine 非阻塞请求后台 Checkpoint；
- hard limit 暂时不足时不 Prepare Batch、不写 Descriptor、不改变 Mapping；
- Context 取消、Prepare 失败、条件冲突、编码失败或 pre-durable append 失败必须释放 reservation；
- durable success 后 Publish 在同一 Mapping 临界区校验 plan、消费实际 charge 并释放 reservation 余量；
- 一旦 Descriptor 允许 durable，Publish 不能再因预算不足失败。

Recovery 同样受 hard limit：它为 replay group 做立即 reservation。若 checkpoint 之后的合法 durable tail
无法放入本次 Open 配置的预算，Open 返回明确配置错误，不能等待一个尚未运行的 Checkpoint，也不能 OOM。

## 5. Pressure 与 Checkpoint 无死锁时序

Store 不再使用覆盖数据面的 `ops` 全局锁。Checkpoint 只使用 Coordinator admission fence，把
`ReserveDelta -> queue` 与 `Freeze` 分在 cut 的两侧。Commit 若持有 admission read lock 等待 Delta
空间，仍会形成：

```text
Commit waits Delta -> Checkpoint waits admission fence -> Delta waits Checkpoint
```

v2 禁止该锁环。Commit 的正确流程是：

```text
1. 进入短生命周期计数并检查 Store lifecycle
2. 在 Coordinator admission read lock 内 reservation、Prepare 并进入 queue
3. queue 接管请求后立即释放 admission read lock；不等待 durable result
4. hard pressure 时不持有 admission lock，通知唯一 Checkpoint worker 并等待对应 Delta generation
5. worker 合并同 generation 请求；Checkpoint 安装释放 frozen charge 并唤醒等待者
6. 重试 admission；成功后再等待 durable Publish
```

步骤 2–3 的 read lock 不能进一步缩短：否则 reservation 成功后、queue admission 之前可能被 Freeze 穿过，
reservation 针对旧 active layer 计算，而 Commit 最终写入新 active layer。它只覆盖无 I/O 的 admission；
durable append 和调用者等待都不持有它。

soft pressure 不阻塞 Commit。`Receipt` 携带 admission 时的 advisory signal；Store 在 admission
完成后通知唯一 Checkpoint worker。hard pressure 同样携带 generation，但调用者等待 worker 覆盖该
generation 后重试。用户 Commit、Relocation、显式 API、周期触发共享同一 worker；调用者不直接构建
Checkpoint。Coordinator barrier 保证 cut 覆盖它之前已 admission 的 Commit。

worker 使用内部 `context.Background()`，不把一个等待者的取消传播给共享任务；调用者 Context 只取消
自己的等待。自动 pressure/periodic Checkpoint 失败时记录 failed counter 并将 Store fail closed，避免
收敛失败被静默吞掉。`Close` 先拒绝新操作并等待已进入的 waiter 完成，再停止 worker，最后关闭
Coordinator 和底层文件。Close 在 drain 时不占有 `checkpointMu` 或 maintenance 锁。

Batch 在步骤 2–4 仍是 Open，因此 Checkpoint 可以把它记入 open-batch cut；它后续的 Commit Record 位于
该 cut 之后。Close 与 Commit 通过 Batch 状态机、Coordinator queue ownership 和 Close drain 协调，不能
依赖 Commit 长时间持有 Store lifecycle RLock。

多个 pressure caller 可以同时请求 Checkpoint；唯一 worker 保证实际构建串行，`checkpointMu` 继续隔离
Mapping GC 的 Root publication 临界区。每个 pressure receipt
携带它被 admission 时所属的 Active Delta generation；Freeze 后的新 Active 使用下一 generation。Store
分别维护 pending 与 completed generation，channel 只负责唤醒：已被显式、hard-pressure 或后台
Checkpoint 覆盖的迟到请求会直接丢弃，只有 cut 后新 Active 上的 pressure 才继续触发下一轮。因此不会
为了已经释放的 Delta 再构建一代空 checkpoint，也不会误丢 cut 后的新压力。

worker 还按 `CheckpointInterval` 定时检查；仅当 Delta charge 非零时才触发周期 Checkpoint，空闲 Store
不会产生空 Manifest/fsync。显式 `Checkpoint` 作为 force request 入队并等待结果。

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
- hard pressure 可以推进 Checkpoint，不与 admission fence 形成等待环；
- soft pressure 在 admission 后非阻塞调度且能释放 Delta charge，合并请求不丢失 post-cut pressure；
- Close 返回前 worker 已退出，后台失败可观测并使 Store fail closed；
- Checkpoint 连续失败时 Commit 停止在 durable 边界之前，Get/Abort/Close 仍可执行；
- replay 超预算确定失败，不等待、不部分发布；
- race test 覆盖 Commit、Checkpoint、Close 和 reservation 取消。

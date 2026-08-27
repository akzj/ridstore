# ridstore v2 M5 Review

状态：Data GC 主路径、候选策略、空间 admission 与 durable fault/crash matrix 已闭合；长期收敛测试进行中

## 1. 本阶段解决的问题

M5 的第一步只实现从 sealed Segment 到当前 active RecordLog 的安全搬迁，不把“复制成功”误当作
“源 Segment 可以删除”。主路径是：

```text
RecordLog.ScanSegment(sealed source, held reader pin)
  -> Decode PutRecord
  -> live iff Mapping[RecordID] == scanned VAddr
  -> Append(original Put payload, sync=false)
  -> shared BatchID allocator
  -> Coordinator.Relocate
  -> CommitGroup(sync=true)
  -> Mapping physical-address CAS
```

复制保留 `RecordID`、Value 和 `OriginBatchID`。Relocation descriptor 使用新的内部 BatchID，并和用户
Commit 共用 CommitSeq、group fsync、Delta reservation 和 Mapping publication。

Engine 返回本轮 relocation descriptor 的 `FirstCommitSeq`/`LastCommitSeq`。后续删除协议必须要求
durable Checkpoint 的 `CoveredCommitSeq >= LastCommitSeq`；不能用“搬迁调用已经返回”替代 checkpoint
覆盖证明。没有 live candidate 时二者均为零。

## 2. 并发语义

扫描时的 Mapping lookup 只是候选判断，不提供排他性。复制后、Coordinator resolve 前如果用户已经
更新同一 ID：

```text
Mapping[ID] != scanned VAddr
  -> relocation mutation skipped
  -> 用户新值保持可见
  -> copied Put 成为不可达 orphan
```

这是预期的 COW 结果，不重试覆盖用户写。测试覆盖了真实 Segment rotation、搬迁成功、
`OriginBatchID` 保留，以及复制与 CAS 之间发生用户更新的竞争。

## 3. 模块边界

- `recordlog.ScanSegment` 只扫描一个 sealed Segment，在 callback 生命周期内持有 Reader Pin，并校验
  RecordLog framing/CRC；它不知道 ridstore record type。
- `engine.RelocateSegment` 拥有 Put 解码、Mapping 判活、批次边界和内部 BatchID 分配。
- `coordinator.Relocate` 仍是唯一 durable publication 入口。
- Catalog 只用于确认 source 是当前 sealed member；本阶段不修改 Segment membership。

搬迁 batch 同时受 `MaxBatchMutations`、`MaxBatchBytes` 和 Commit descriptor payload 上限约束。没有
新增兼容路径、adapter 或第二套 GC mapping protocol。

## 4. 退休前证明

`PrepareSegmentRetirement` 在单一 maintenance gate 内执行：

1. 搬迁当前 live Put；
2. 创建覆盖 `LastCommitSeq` 的 durable Checkpoint；
3. 取得 Engine exclusive operation gate；
4. 拒绝仍以最终 Put mutation 引用 source 的 Open/Committing Batch；
5. 验证 checkpoint sparse stats 对 source 为零；
6. 再扫描 source，并逐条确认当前 `Mapping[ID] != scanned VAddr`；
7. 返回绑定 source summary、Catalog generation、CoveredCommitSeq 和 ReplayStart 的 proof。

历史 Put 若已被同 Batch 的 Put/Delete 覆盖，不会进入 Mapping，因此不属于 open-batch 引用。proof 不是
独立删除令牌；未来 retire 操作必须在 maintenance/operation gate 内消费并重新验证。

`CompactSegment` 在 proof 之后安装唯一 durable `MAINTENANCE.v2` marker，再调用 RecordLog retire gate、
Catalog remove、Registry detach、close、trash 和 delete。marker 删除是最后一步。

恢复不持久化内存阶段，而只比较 marker 与 Catalog：

- source 仍在 BaseGeneration Catalog：不可逆操作尚未发生，删除 marker；
- source 已从 BaseGeneration+1 Catalog 移除：继续幂等物理清理，再删除 marker；
- 其他 generation、identity 或 summary 组合：corruption，拒绝猜测。

fault hook 已覆盖 marker 的 temp cleanup/create、write、file sync、close、rename、directory sync 和 remove，
以及 source-to-trash rename、records/trash directory sync、trash unlink 和最终 directory sync。marker 已
rename 但目录 sync 失败时，Engine 将其视为 outcome unknown，进入 `RecoveryRequired`，禁止在当前实例
继续 checkpoint 或维护。

子进程退出测试固定了四个恢复状态：marker durable/Catalog present、Catalog removed/canonical present、
source in trash、trash deleted/marker present。fresh Open 均只依赖 marker + Catalog 收敛，并验证搬迁后
的值仍可读取。

跨目录 rename 的重试有一个不可省略的顺序：只要发现 trash 文件，在 unlink 前都重新 fsync
`records/` 和 `trash/`。否则上次 rename 的目录同步失败后，当前进程删除 marker，再次掉电仍可能让
canonical source 重新出现。

Transaction 可以从最终 mutation 集合报告 Segment 引用。被后续 Put/Delete 覆盖的历史 Put 不会进入
Mapping，因此不阻塞 GC；Open/Committing Batch 的最终 Put 在 durable publication 或终止清理前持续
构成引用。退休前证明在阻止新 Batch 操作后检查该引用集合。

## 5. Sparse SegmentStats 语义修正

Checkpoint builder 只编码含 live Record 的 sealed Segment，因此表项缺失在同一
`StatsCoveredCommitSeq == CoveredCommitSeq` 的 Manifest 中明确表示零存活。Catalog retire 门禁已按
这一格式解释：缺失或显式零值允许继续，任何非零值拒绝。它仍只是必要条件；Engine 的二次 Mapping
证明与 open-batch gate 仍是物理 retire 的前置条件。

## 6. M5 剩余范围

当前缺口不再是“能否安全删除一个已指定 Segment”，而是长期并发与收敛证据：

- relocation 与前台更新交错的模型测试；
- 重复 GC、重启和空间回收的长时 convergence soak。

这些工作不能削弱当前删除门禁；SegmentStats 仍只筛选候选，不能独立授权删除。

### 6.1 已完成的候选选择

`CompactNextSegment` 先生成 exact Checkpoint，再以单遍、有界工作空间选择至多一个 Segment。候选必须：

- sealed Segment 的 end 不晚于 ReplayStart；
- 不被任何 open Batch 的最终 Put 引用；
- 同时满足调用者提供的最小 reclaimable bytes 与最小 reclaimable ratio；
- 在多个候选中优先 reclaimable bytes 最大、live bytes 更小、SegmentID 更早者。

候选结果明确携带 Catalog generation 和 StatsCoveredCommitSeq，但只是调度提示。执行阶段仍重新 relocation、
Checkpoint 和 exact proof；没有把统计结果升级为删除令牌。

### 6.2 已完成的资源门禁

- relocation batch 同时受 runtime bytes/mutations、持久化 Batch 上限和 descriptor 上限约束；
- copy 前保守预算 source、descriptor 与 rotation 空间；
- relocation 后按实际 frozen Delta entry 上界预算最坏八层 Dense Mapping COW；
- 第二阶段空间不足不会退休 source，也不会撤销已经 durable 的 relocation；
- 每个 durable relocation batch 后按累计 copied physical bytes 限速，等待支持 context 取消；
- 导出 GC throttled time 与 space rejection 计数。

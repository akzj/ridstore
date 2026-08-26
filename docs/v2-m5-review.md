# ridstore v2 M5 Review

状态：Relocation 与退休前证明已闭合；物理删除尚未接入

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

尚未完成的是可恢复 maintenance journal、retire gate/Catalog remove/trash/delete 的组合以及对应
crash/syscall matrix。在这些条件闭合前，Engine 不调用 `RecordLog.RetireSegment`。

Transaction 可以从最终 mutation 集合报告 Segment 引用。被后续 Put/Delete 覆盖的历史 Put 不会进入
Mapping，因此不阻塞 GC；Open/Committing Batch 的最终 Put 在 durable publication 或终止清理前持续
构成引用。退休前证明在阻止新 Batch 操作后检查该引用集合。

## 5. Sparse SegmentStats 语义修正

Checkpoint builder 只编码含 live Record 的 sealed Segment，因此表项缺失在同一
`StatsCoveredCommitSeq == CoveredCommitSeq` 的 Manifest 中明确表示零存活。Catalog retire 门禁已按
这一格式解释：缺失或显式零值允许继续，任何非零值拒绝。它仍只是必要条件；Engine 的二次 Mapping
证明、open-batch gate 和可恢复 maintenance 协议尚未完成时，不调用物理 retire。

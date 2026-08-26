# ridstore v2 M5 Review

状态：Relocation 主路径已闭合；Segment 删除尚未授权

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

## 4. 明确尚未完成的安全门

`RelocateSegment` 成功只证明每个已 applied 地址更新拥有 durable descriptor。源 Segment 删除前仍需：

1. 取得覆盖全部 relocation CommitSeq 的 Checkpoint；
2. 针对该 source 执行 checkpoint 后的精确 Mapping liveness 证明；
3. 处理 open-batch 对 source 中未提交 Put 的引用；
4. 建立可恢复的 maintenance/retire 状态；
5. 进入 retire gate，等待 Reader Pin 清零；
6. 原子移除 Catalog membership，再 detach、close、trash、delete；
7. 完成上述每个边界的 crash/syscall matrix。

在这些条件闭合前，Engine 不调用 `RecordLog.RetireSegment`。

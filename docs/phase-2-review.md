# Phase 2 全局 Review

状态：Passed（2026-08-21）

## 1. Review 结论

Phase 2 已把 Phase 1 的互斥退化实现替换为具备明确所有权的并发流水线：

```text
caller -> bounded append queue -> append sequencer
caller -> bounded commit queue -> group validation -> one group append/fsync
       -> CommitSeq ordered Mapping publish -> per-Batch result
```

并发不会改变单 Batch 原子性。一次共享 fsync 只共享 durability boundary，不共享条件、
CommitSeq、Mapping mutation 或返回状态。

## 2. 已验证不变量

- append sequencer goroutine 是 FrameSeq、VAddr 和 Active rotation 顺序的唯一所有者；
- commit coordinator goroutine 是 CommitSeq、group validation 和 publish 顺序的唯一所有者；
- group 使用 virtual Mapping，后一个 request 看见同 group 前一个 admitted request，冲突 Batch 不占 CommitSeq；
- 全 group Descriptor 在写入前完成编码与容量预检，一次 `fsync` 后才开始 Mapping publish；
- 一个 Batch 冲突只使自身 Aborted，不错误确认或撤销 group 中其他 Batch；
- Seal 写入开始后的 group I/O 错误按每个 Batch 的实际边界分类为 Unknown/Aborted；
- Context 在入队或 validation 阶段取消为确定 Aborted；请求一旦被队列接纳，调用者等待最终分类，不提前释放 Batch；
- Close 先取得 Store lifecycle 写锁，等待公开操作完成，再依次关闭 coordinator、sequencer、Segment Registry 和目录锁；
- queue、open Batch 和 group 大小均有配置上限；blocked Begin 可被 Context 或 Close 唤醒；
- Open Batch 对写入过的每个 Segment 持唯一 ref，终态统一释放，为 Phase 4 删除门禁提供依据。

## 3. Segment rotation

rotation 在 sequencer 内执行，普通 Frame 始终预留 terminal SegmentSeal 空间：

```text
ROTATION journal durable
-> old SegmentSeal + footer durable
-> old rename + data dir fsync
-> new Active header durable
-> Manifest(old sealed + new active) install
-> remove journal
```

读取通过 Segment Registry 解析 Active/Sealed VAddr。Open 会先完成残留 ROTATION，再打开
Manifest 文件集合；恢复按 SegmentID/FrameSeq 顺序跨 Segment 重放。

子进程 SIGKILL matrix 已覆盖 prepared、old sealed、new created、Manifest installed 四个边界。
测试不调用 Close/Flush，重启后必须能够继续 durable Commit。

## 4. 并发与可观测性证据

- 64 个预先打开的 Batch 同时 Commit，确认 `GroupBatches > CommitGroups`；
- 关闭重开后逐 ID 校验全部值；
- race suite 覆盖 sequencer、coordinator、Mapping、rotation 和 Close；
- `Store.Metrics()` 暴露有界原子累计值：queue wait、validation、write+sync、publish、group size 和终态计数；
- 指标写入不参与 admission、durability 或恢复决策。

## 5. 仍未完成

- Mapping 仍为 memory oracle，Open 仍扫描完整 Data Log；
- 没有 Delta hard limit、Persistent Root、Checkpoint 或 Mapping Cache；
- SegmentStats 尚未生成，Data Segment 只增加不回收；
- Reader pin/Retire 和真实删除属于 Phase 4；
- metrics export adapter、长时 fuzz、72h soak 和对比 benchmark 属于 Phase 5。

因此 Phase 2 可以进入 Persistent Mapping，但不能声明生产就绪。

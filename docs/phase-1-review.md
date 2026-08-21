# Phase 1 全局 Review

状态：Passed（2026-08-21）

## 1. Review 结论

Phase 1 的最小 durable Record Store 主路径已经闭合，可以进入 Phase 2：

- `PutRecord` 先顺序 append，只有完整 `CommitSeal` 才使 Batch 可恢复为 Committed；
- `CommitSeal` 所在写入完成一次 `fsync` 后才发布 Mapping 和返回成功；
- 运行时与恢复都按 CommitSeq 严格递增、按 Descriptor 的最终 mutation 集发布；
- orphan Put、未完成 Descriptor 和 durable reserve 中未发放的 ID 不会被重新使用或错误发布；
- Active 尾部只修复不完整 Frame，完整但 CRC 错误的 Frame fail closed；
- lost response 返回 `CommitUnknown`，重启以 durable Seal 决定最终结果；
- Close 等待正在执行的公开操作退出，然后清理 Open Batch、关闭文件并释放目录锁。

这次 Review 不声明并发 group commit、Segment rotation、Persistent Mapping、Checkpoint 或 GC 已经完成。

## 2. 不变量核对

### Commit 与恢复

- Commit Descriptor 写入前验证所有 PutRecord 的 `RecordID/OriginBatchID/ValueBytes/PhysicalSize`；
- 条件冲突不分配 CommitSeq、不写 Seal，并使 Batch 确定 Aborted；
- Seal 写入开始后的失败统一返回 `ErrCommitUnknown`；
- fsync 成功后 Mapping 发布失败会使 Store fail closed，重启从 Seal 重放；
- Recovery 只发布通过完整 Part/Seal/CRC/PutRecord 关联校验的 Descriptor；
- CommitSeq 允许崩溃形成空洞，但已出现的 Seal 必须严格递增。

### 分配器

- ID 和 BatchID 只从已经 durable 的 reserve range 发放；
- Open 时从扫描到的最高 durable reserve 之后继续，整个已持久化 range 都不复用；
- `Status` 将重启后 reserve 内无法确认已发放的 BatchID保守解释为 Aborted，而不是 Committed。

### 锁与生命周期

当前嵌套顺序为：

```text
Store.ops RLock
  -> Batch/Allocator mutex
    -> appendlog mutex
      -> ActiveData mutex
```

Commit Coordinator mutex 位于 Batch `Prepare` 之外，持有期间不会调用用户代码。`Close` 取得
`Store.ops` 写锁，因此不会在仍有公开操作访问文件时关闭 FD。`Store.mu` 不跨磁盘 I/O。

## 3. Crash Matrix

子进程在目标 failpoint 停住后由父进程 `SIGKILL`，子进程不调用 Close/Flush：

| 边界 | 重启结果 |
|---|---|
| PutRecord written | Aborted |
| CommitPart written | Aborted |
| CommitSeal written, before sync | Committed 或 Aborted |
| Commit sync returned | Committed |
| Mapping published | Committed |
| Result ready, response lost | Committed |

该测试模拟进程崩溃，不声称覆盖设备写缓存掉电语义。

## 4. 已知边界

- 一个 Active Data Segment；空间不足返回确定未提交的 `ErrFull`；
- Recovery 遇到 Sealed Data Segment、Persistent Mapping Root 或 Relocation 会拒绝打开；
- Commit 仍是串行单请求 fsync，没有 group commit；
- Mapping 是全内存 oracle，Open 必须扫描 Active Segment；
- `Checkpoint` 返回 `ErrUnsupported`；
- Status 保留表尚未做容量回收；
- 尚未完成长期 fuzz、72 小时 soak 和同 durability benchmark。

这些边界分别由 Phase 2–5 消除，不通过扩大 Phase 1 的隐式行为来掩盖。

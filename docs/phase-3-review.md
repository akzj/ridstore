# Phase 3 Persistent Mapping 全局 Review

状态：通过，可以进入 Phase 4 Data GC；不构成 production-ready 声明。

日期：2026-08-21

## 1. 已完成的主路径

- 生产默认 Mapping 为 8 层、9-bit stride 的 Persistent Radix，memory Mapping 只保留为 oracle；
- SparseBitmap/Dense512 codec、完整 uint64 ID 路径、Persistent Root 冷读与有界 LRU Cache 已实现；
- 同地址 Cache miss 合并，避免并发冷读重复放大；
- Commit queue barrier 原子取得 durable Data cut、CommitSeq cut 与 frozen Delta，新 Commit 在 Root 构建期间继续进入新 active Delta；
- Checkpoint 采用脏路径 bottom-up COW，不再全树重写；Root 可以引用更早 checkpoint 的 immutable 子树；
- Builder 按 `CheckpointMemoryBytes` 分 chunk；SegmentStats 增量应用 folded changes，active 转动后
  顺序扫描 former-active segment 并与 candidate Mapping join；
- Root、Stats、ReplayStart、allocator high watermark、Open Batch IDs 由同一 Manifest generation 安装；
- active+frozen+admitted reservation 纳入 Delta hard limit，soft limit 自动调度 Checkpoint；
- Node/Delta/Checkpoint cancellation、Close 与 Manifest 安装后的 fail-closed 路径均有自动化覆盖；
- `CompactMapping` 使用独立 Mapping generation、Root reader pin、MAINTENANCE Journal 与 trash/delete 回收旧文件；
- `MappingSpaceUsage` 可精确报告 encoded reachable/unreachable Node bytes，并有空间收敛测试。

## 2. Commit、Checkpoint 与恢复不变量

Checkpoint barrier 在 commit coordinator FIFO 中执行。barrier 之前的 Commit 必须完成 Mapping publish；barrier 之后的 Commit 只能进入新 active Delta。`CoveredCommitSeq + 1 == NextCommitSeq` 不成立时 Store fail closed。

Manifest durable 之前，旧 Root 和全部 frozen layer 仍是运行时真相；Manifest durable 之后，任何 runtime publish 失败都要求重启采用新 Manifest。恢复从 Manifest Root/ReplayStart 开始重放，因此与运行时得到同一 Mapping。

Delta reservation 在 pre-Seal 阶段取得；取消、冲突、append 失败和 coordinator fault 都释放 reservation。Descriptor durable 后的 Mapping publish 不再等待预算，避免 durable Commit 与 Checkpoint 相互等待。

## 3. Mapping GC 删除门禁

Mapping GC 顺序固定为：

```text
pin old Root
-> copy reachable tree to new file generation
-> fsync files and directories
-> install Manifest(new Root and complete mapping file set)
-> adopt runtime Root
-> release GC pin and wait all old Root readers
-> rename old files to trash and fsync directories
-> unlink trash and fsync
-> remove MAINTENANCE journal
```

Manifest 安装前崩溃会删除临时/未引用 destination；安装后崩溃会先完整遍历验证新 Root，再继续回收旧 generation。Segment ID 不复用，因此旧 Cache entry 不会与新 Node 地址混淆。

## 4. 自动化证据

- memory/radix 随机模型一致性；
- 高位 ID、Delete path pruning、单脏 Leaf 只重写 8 层路径；
- Cache miss 合并与错误重试；
- Delta hard-limit 等待、取消归还、soft-limit 自动 Checkpoint；
- Checkpoint 与并发 Commit、Close、恢复；
- Mapping rotation 与 Checkpoint subprocess SIGKILL matrix；
- Mapping GC Prepared、Copying、Copied、FilesDurable、ManifestInstalled、RuntimeInstalled、Trashed subprocess SIGKILL matrix；
- Mapping GC 等待旧 Root reader、并发 Commit Delta、关闭重开、exact space convergence；
- `go test ./...`、`go test -race ./...` 与 `go vet ./...`。

## 5. 进入 Phase 4 前的明确边界

当前 Mapping 使用单一 RWMutex 保护 active/frozen publication，而不是设计文档中可选的 shard 优化。普通 Root I/O 已在锁外；该实现优先保证全 Batch 原子可见，后续只有基准证明必要时才分片。

`ApplyRelocation` 目前仍会在 Mapping 写锁内解析 expected-old 状态。Phase 4 在接入真实 Relocation 前必须改为 coordinator pre-resolve plan：冷 Root Lookup 在锁外完成，fsync 后 publisher 只执行已解析的纯内存 apply/skip，并在前提不一致时 fail closed。

SegmentStats 目前是 Checkpoint 同步精确派生状态；它可以筛选 Data GC 候选，但永远不能授权删除。Data Segment 删除仍必须完成 Reader pin、Relocation CAS、post-copy exact validation、GC-required Checkpoint、Manifest remove 和 trash 协议。

## 6. 尚未完成

- Phase 4 Data GC、Reader pin/Retire、Relocation recovery 与 ENOSPC 收敛；
- Phase 5 verify/scrub、backup/restore、migration、长期 fuzz、72h soak 和同 durability benchmark；
- 自动 Mapping GC 调度策略；当前提供显式、可审计的 `CompactMapping`。

因此 Phase 3 的 durable Mapping 与空间回收基础已经闭合，但整个项目仍不能称为生产就绪。

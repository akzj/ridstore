# 实施计划

状态：Execution plan v1

## 1. 开发原则

- 每阶段先冻结契约和失败时序，再写实现；
- 最小主路径优先，不同时扩展 Page/Blob/KV/Stream；
- 模块通过窄接口组合，不能依赖 goroutine ID 或隐式全局上下文；
- 每个模块完成后独立 commit；
- Commit 前审计锁顺序、错误传播、取消清理、格式兼容和完整 diff；
- 测试通过不替代协议 Review；
- 未完成的环境/长时验证必须如实报告。

## 2. 建议代码结构

```text
ridstore/
  ridstore.go                 public API and types
  errors.go
  internal/
    format/                   binary codecs only
    filelock/                 data directory ownership
    manifest/                 CURRENT and Manifest install
    segment/                  append/read/seal/registry/pin
    appendlog/                frame sequencing and rotation
    batch/                    batch state and mutation folding
    commit/                   group commit coordinator
    allocator/                stable ID reserve
    mapping/
      api/                    mapping contract
      memory/                 oracle/reference implementation
      radix/                  persistent radix and cache
    checkpoint/               overlay cut and root install
    recovery/                 open scan and replay
    gc/                       relocation and retire journal
    metrics/                  bounded observability
    failpoint/                test-only injection
  cmd/
    ridstore-tool/            offline verify/scrub; later phase
  test/
    crash/                    subprocess black-box suite
    integration/
  docs/
```

`internal/format` 不调用文件系统；`segment` 不理解 Mapping；`mapping` 不理解用户 payload；`gc` 只能通过公开内部接口协调这些模块。

## 3. 锁与 goroutine 所有权

预先固定所有权：

- append sequencer goroutine：唯一分配 FrameSeq 和 Data VAddr；
- commit coordinator goroutine：唯一分配 CommitSeq 和执行 group fsync；
- checkpoint coordinator：同一时刻最多一个；
- GC coordinator：第一版同一时刻最多一个 Data GC；
- Mapping publish lock：只保护短内存发布；
- Segment Registry lock：只保护状态/pin/ref，不执行磁盘 I/O；
- Store lifecycle lock：Open/closing/closed/faulted。

禁止：

- 在 Mapping publish lock 下 fsync；
- 在 Segment Registry lock 下复制 payload；
- 通过 goroutine ID 查找事务或 collector；
- 后台 goroutine 忽略错误继续推进删除边界；
- Close 设置超时后遗留仍访问已关闭文件的 goroutine。

## 4. Phase 0：格式与 Harness

模块/提交顺序：

1. 项目骨架、Go module、Makefile、lint/test 入口；
2. 基础 ID/VAddr/FrameSeq/CommitSeq 类型和 checked arithmetic；
3. Data/Mapping Segment Header/Footer codec；
4. Data Frame codec 与 PutRecord OriginBatchID 语义；
5. Commit/Relocation Descriptor codec；
6. Mapping Node codec；
7. Manifest/CURRENT codec 与原子安装；
8. INITIALIZING/ROTATION/MAINTENANCE Journal codec；
9. 文件锁与可恢复 Create/Open 初始化；
10. failpoint framework；
11. subprocess crash harness；
12. golden vectors、fuzz seeds 和 Format Freeze Review。

不在 Phase 0 写 B-Tree、GC 或性能优化。

完成定义见 `verification-plan.md`。

## 5. Phase 1：最小 durable Record Store

模块/提交顺序：

1. 单 Active Data Segment append/read；
2. Active tail scan/truncate 和 Sealed strict validation；
3. memoryMapping oracle；
4. IDReserve 与 BatchIDReserve；
5. Batch Begin/Allocate/Put/Delete/Abort、LogicalRevision 和条件集合；
6. 单 Batch Commit Descriptor + fsync；
7. Mapping Publish；
8. Store Get/GetRecord 与单请求条件验证；
9. Open Recovery；
10. Batch Status/CommitUnknown；
11. Close 和故障状态。

此阶段 append sequencer/commit coordinator 可以退化为单请求，但协议和接口必须与 Phase 2 兼容。

完成后进行一次全局 Review：正常 Commit、lost response、process crash、ID reserve、torn tail。

## 6. Phase 2：并发与 group commit

模块/提交顺序：

1. append request queue 和唯一 sequencer；
2. Open Batch 配额和 Segment refs；
3. commit request queue；
4. group virtual Mapping 条件验证 + group write/fsync；
5. CommitSeq 顺序发布；
6. Segment rotation；
7. Context cancel/Close 协调；
8. backpressure；
9. 分段 latency 和 queue metrics；
10. 并发模型/基准/race。

验收重点：共享 fsync 不合并 Batch 原子性；一个 Batch 错误不错误确认同 group 其他 Batch。

## 7. Phase 3：Persistent Mapping

模块/提交顺序：

1. Mapping interface 模型测试；
2. Map Segment/MapAddr；
3. Radix Node codec/path validation；
4. bounded Node Cache；
5. Persistent Root Lookup；
6. Delta Overlay；
7. atomic Mapping State/Publish；
8. checkpoint overlay cut；
9. bottom-up COW builder；
10. Mapping Segment rotation journal + files fsync + Manifest Root install；
11. Recovery from Root + replay；
12. Delta limits/backpressure；
13. Mapping GC。

切换生产默认前，memoryMapping 与 radixMapping 必须通过同一随机模型测试。

## 8. Phase 4：Data GC

模块/提交顺序：

1. Segment live/dead accounting；
2. Reader pin/Retire state machine；
3. GC candidate selection；
4. live Record copy；
5. Relocation Descriptor/fsync；
6. runtime/recovery CAS；
7. post-copy exact validation；
8. GC-required Mapping Checkpoint；
9. Maintenance Journal；
10. Manifest remove/rename-to-trash/delete；
11. crash resume；
12. ENOSPC/backpressure；
13. convergence soak。

不得在 Relocation/Checkpoint/Pin 协议完成前实现真实文件删除。

## 9. Phase 5：完整性与运维

1. offline `ridstore-tool verify`；
2. scrub report；
3. consistent backup protocol；
4. restore 到新目录并验证 UUID 策略；
5. metrics export adapter；
6. format upgrade/migration skeleton；
7. full crash matrix；
8. long fuzz/nightly；
9. 72h soak；
10. same-durability RocksDB/Pebble benchmark；
11. known limits 和 production checklist。

独立进程 `ridstored`、复制和 HA 不在该计划内。

## 10. 每模块提交要求

每个模块 commit 前：

- 只包含该模块及必要契约更新；
- 新 public/internal interface 有编译期实现检查；
- 错误路径和 cancellation cleanup 有测试；
- format 变化更新 golden vectors 和版本；
- 不忽略 fsync/rename/close 错误；
- 不持锁执行未知时长回调；
- `go test`、目标包 `-race`、`go vet` 通过；
- 检查 goroutine、FD、临时文件泄漏；
- 更新实施计划状态和已知限制。

## 11. 全局 Review 节点

以下节点必须暂停局部开发，进行全局架构 Review：

- Format Freeze；
- Phase 1 首次 durable Commit；
- Phase 2 group commit；
- Persistent Mapping 成为默认；
- 第一次真实删除 Data Segment；
- 第一次 Backup/Restore；
- production-ready 声明前。

Review 至少检查：

- 核心不变量是否仍成立；
- Commit/Recovery 是否同一语义；
- Checkpoint 与 GC 删除边界；
- 锁顺序和 Close；
- 文档与代码是否一致；
- 是否开始漂移成通用 KV/LSM；
- 性能优化是否削弱 durability。

## 12. 暂不实现

- Page/Blob/Stream 专用类型；
- 任意 Key、Range Scan、SSTable、Level Compaction；
- SQL；
- TTL；
- 多目录分片；
- 独立 daemon；
- 主备、Raft、Quorum Commit；
- 分布式 ID；
- 跨 Store 事务；
- SyncNone production mode。

这些需求若出现，先更新项目定位和独立设计，不作为“继续开发”的隐式内容。

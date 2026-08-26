# ridstore v2 模块处置矩阵

状态：Active migration ledger

本文决定现有代码进入 v2 主路径时是 `Keep`、`Rewrite` 还是 `Delete`。不存在 `Adapt`。

## 1. 判定规则

`Keep` 必须同时满足：

- 目标职责与当前职责一致；
- 不产生第二个状态或生命周期所有者；
- 不需要兼容旧接口、旧格式或旧并发模型；
- 错误和崩溃语义满足 v2；
- 测试能够直接验证新契约，而不是只锁定旧实现。

任一条件不满足即 `Rewrite`。新实现稳定后，旧实现 `Delete`。禁止长期 wrapper、bridge、dual-write
或新旧格式分支。

## 2. 当前初步矩阵

| 当前模块 | 处置 | v2 中的职责或理由 |
|---|---|---|
| Public `Store`/`Batch` API 契约 | Rewrite | 保留 Stable-ID 与原子 Batch；删除 LogicalRevision，条件提交只比较 Mapping 当前 VAddr，公共层至多暴露 opaque observation token |
| `internal/base` | Shrink at M6 | v2 暂时共享错误；旧 ID、Revision、VAddr、LogPos 类型在 v1 删除后移除 |
| `internal/filelock` | Keep | 单目录进程级独占锁与 v2 生命周期职责完全一致；v2 Open 在任何可变恢复前取得锁，Close 最后释放 |
| `internal/bootstrap` | Keep | v2 初始化唯一所有者；以初始 Manifest 编码作为恢复 marker |
| `internal/allocator` | Delete at M6 | 已由 `internal/idalloc` 替代 |
| `internal/batch` | Delete at M6 | 已由 `internal/transaction` 替代 |
| `internal/commit` | Delete at M6 | 已由 `internal/coordinator` 替代 |
| `internal/appendlog` | Delete | 旧 Sequencer、业务 Frame 构造和 buffer 与 RecordLog v2 重复 |
| `internal/recordlog` | Keep | v2 唯一物理日志：有界队列、batching、VAddr、Segment、rotation、Reader Pin 与恢复 |
| `internal/segment` | Delete at M6 | 物理读、Reader Pin 和 rotation 已由 `internal/recordlog` 原生拥有 |
| `internal/rotation` | Delete at M6 | rotation 已分别由 RecordLog 和 MapStore 原生拥有 |
| `internal/catalog` | Delete at M6 | 已由 `internal/storecatalog` 替代 |
| `internal/manifest` | Delete at M6 | Manifest codec/install 已由 `internal/storecatalog` 替代 |
| `internal/format` | Delete at M6 | v2 格式已拆分到 `recordcodec`、`recordlog`、`mapstore`、`storecatalog` |
| `internal/mapping/api` | Delete at M6 | 已由 `internal/mapping` 的单一 Index 契约替代 |
| `internal/mapping/memory` | Delete at M6 | v2 测试模型已位于 `internal/mapping`，不保留第二套 Mapping |
| `internal/mapping/radix` | Delete at M6 | 已由 `internal/mapping`、`internal/radix`、`internal/mapstore` 替代 |
| `internal/recovery` | Delete at M6 | 已由 `internal/replay` 替代 |
| `data_gc.go` | Rewrite | 保留 liveness/CAS/Reader Pin/删除顺序，不保留旧 Segment 和 Frame 接口 |
| `internal/maintenance` | Rewrite | journal 机制可借鉴，但状态字段必须原生服从 v2 Catalog |
| `internal/backup` | Rewrite | 文件集合与格式改变，不允许在旧清单结构上补兼容分支 |
| `internal/verify` | Rewrite | verifier 必须原生理解 Format v2，不能双格式猜测 |
| metrics/export | Rewrite | 指标名称可以复用；采集结构直接围绕 v2 水位和唯一 writer 生成 |
| fault/crash/model tests | Keep 测试意图 | 测试代码按新接口重写，故障矩阵和性质继续作为验收标准 |

矩阵中的决定面向最终主路径。Rewrite 可以复用经过证明的不变量、算法和测试性质，但不复制旧
类型边界，也不通过 wrapper 让旧实现继续参与运行。

## 3. 明确删除的概念

v2 不再存在：

- 旧 `appendlog.Sequencer`；
- 两层 append queue；
- `FrameSeq` 与 VAddr 两套物理顺序；
- CommitPart/CommitSeal 邻接协议；
- ActiveData 内再次编码业务 Frame；
- old/new AppendLog fallback；
- Format v1 在线兼容；
- 生产配置选择旧、新引擎；
- 为复用旧测试而暴露的内部接口。

## 4. RecordLog 验收性质

可以保留的知识和验证：

- 单 writer channel；
- bounded request/byte budget；
- 自然形成的 write/fsync batching；
- stable VAddr reservation；
- pending index；
- reserved/written/durable watermarks；
- size-tag 读取提示；
- short-write/fsync failure 后 fail-closed；
- active tail repair；
- golden/fuzz/model/crash/syscall-count/benchmark 方法。

正式实现位于 `internal/recordlog`。Catalog 是 Segment membership 的唯一 durable 权威，RecordLog
负责物理 append、读取、rotation、Registry 与 retire 执行，不保存第二份 Manifest。

## 5. 实施次序

### M0：文档冻结

- Review 总体架构、Recovery 和本矩阵；
- 冻结 RecordLog API、Manifest v2 schema 和 Protocol Record；
- 明确最大 CommitGroupRecord 与 Segment 的约束。

### M1：v2 基础类型与格式

- 生成新的 VAddr、LogPos、checked arithmetic 和错误分类；
- 生成 RecordLog envelope、Ridstore Protocol 和 Manifest v2 codec；
- 为所有 decoder 建立 golden、boundary、corruption 和 fuzz 测试；
- 生成只接受 Manifest v2 的单一 Catalog；
- 新代码使用最终职责命名，不创建长期 `v2compat` 或 adapter 包。

### M2：全新 RecordLog

- 在新包中生成正式实现，不修改旧 appendlog；
- 实现统一 Append、唯一 writer、Segment、Registry、rotation、Catalog publication 和物理恢复；
- 迁移 v2 原型的性质测试而不是复制旧 API；
- M2 完成前旧路径仍可编译，但不与新路径互相调用。

### M3：Ridstore 最小闭环

- 重写 Coordinator；
- 接入 Mapping；
- 完成 Create/Update/Delete/Get 与原子 Batch；
- 建立全新 v2 runtime，不能通过 adapter 调旧 appendlog。

### M4：Checkpoint 与 Recovery

- 接入 Mapping checkpoint；
- 重写 replay；
- 完成 crash matrix 后删除旧 recovery/format 主路径。

### M5：GC 与维护

- SegmentStats 已由 Checkpoint 精确派生；
- Relocation 已接入 sealed Segment pin/scan、共享 BatchID、唯一 Coordinator 和 Mapping VAddr CAS；
- Checkpoint coverage、open-batch gate 和二次精确 liveness 证明已组合为退休前 proof；
- 最小 durable maintenance marker、retire gate、Catalog remove、trash/delete 与 Open 恢复已接入；
- 完成 GC crash matrix 和 model test。

### M6：删除旧系统

- 删除 `internal/appendlog` 和不再引用的 Format v1 组件；
- 删除旧 runtime、rotation、recovery 和兼容测试；
- `rg` 验证不存在 fallback、legacy、v1 runtime 分支；
- 全量 race/fuzz/crash/soak 后才允许把 v2 作为 main 候选。

## 6. 每个模块开工前的问题

1. 它在目标架构中是否只有一个所有者？
2. 它的输入输出是否包含不属于它的业务语义？
3. 它是否要求另一个模块维护镜像状态？
4. 崩溃后由哪一个 durable 证据恢复？
5. 如果删除现有实现重新生成，模型是否会更简单？

第 5 个问题答案为“是”时，必须选择 Rewrite，不能因为局部改动更快而保留旧代码。

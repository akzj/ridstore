# ridstore v2 模块处置矩阵

状态：Draft for Review

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
| Public `Store`/`Batch` API 契约 | Keep | Stable-ID、Batch、LogicalRevision 的产品语义不变 |
| `internal/base` | Rewrite | 保留 ID 和错误语义；重新生成 VAddr、LogPos 和边界检查，避免共享旧地址编码器 |
| `internal/allocator` | Rewrite | 保留 durable high watermark 不变量；直接依赖新 ReserveRecord 契约 |
| `internal/batch` | Rewrite | 保留状态机和最终 mutation 折叠语义；删除旧 Frame/AppendLog 依赖 |
| `internal/commit` | Rewrite | 保留冲突验证和 Mapping 发布不变量，但删除旧 AppendLog/FramePart 接口 |
| `internal/appendlog` | Delete | 旧 Sequencer、业务 Frame 构造和 buffer 与 RecordLog v2 重复 |
| `internal/appendlog/v2/budget.go` | Keep | 有界 byte budget 独立于业务和旧执行路径 |
| `internal/appendlog/v2/fileio.go` | Keep | 完整 I/O 与抽象文件后端符合物理层职责 |
| `internal/appendlog/v2` 其余实现 | Rewrite 为正式 RecordLog | 复用设计、测试和算法；不把原型 API 和目录所有权直接提升为生产接口 |
| `internal/segment` | Rewrite | 物理读、Reader Pin 有价值，但写入/rotation 所有权要归 RecordLog 子系统 |
| `internal/rotation` | Rewrite | 保留 journal 状态机思想；按 v2 Catalog/RecordLog 所有权重新生成 |
| `internal/catalog` | Rewrite | 保留单一 generation owner 原则；直接生成 Manifest v2 字段所有权模型 |
| `internal/manifest` | Rewrite | 保留原子安装协议；不在 Format v1 的具体类型上增加分支 |
| `internal/format` | Delete 后重建 v2 codec | Format v1 Frame/CommitPart/Seal 不进入 v2 生产路径 |
| `internal/mapping/api` | Keep | `ID -> VAddr`、resolved publish、checkpoint 契约保持 |
| `internal/mapping/memory` | Keep | 只实现 Mapping API，不拥有持久化格式或追加生命周期 |
| `internal/mapping/radix` | Rewrite | 保留 COW Root、Delta、Cache 算法和测试性质；原实现绑定 Format v1/Catalog |
| `internal/recovery` | Rewrite | v2 使用 RecordLog envelope + 单 CommitGroupRecord，不保留 FramePart 状态机 |
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

## 4. 从 appendlog/v2 原型提取什么

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

不能因为已经实现就直接保留：

- 当前同步 `Append` API 可以保留；它的 Close、取消和数据所有权仍需按正式契约重新验证；
- 当前目录扫描即权威的 open 方式；
- 当前独立 Segment membership；
- 当前 rotation publication；
- 与全局 Catalog 重复的文件状态；
- 不支持 GC retire 的 sealed 文件生命周期。

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

- 重写 SegmentStats、Relocation 和 retire/delete；
- 接入 Reader Pin；
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

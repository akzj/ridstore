# Durable Writer Syscall Fault Matrix

状态：Phase 5 production audit，持续更新。

本表区分两类证据：phase crash failpoint 证明进程在某个已完成阶段退出后的恢复；`before-*` syscall failpoint 证明底层调用直接返回 `ENOSPC/EIO/EACCES` 时，错误 cause、内存状态和 fresh Open 语义正确。前者不能替代后者。

| Writer | write | file sync | rename/remove | directory sync | 当前结论 |
|---|---|---|---|---|---|
| Manifest + CURRENT publication | 已覆盖 | 已覆盖 | 已覆盖 | 已覆盖 | 完整 syscall matrix；不确定 publication 后运行时 fail closed，fresh Open 以 CURRENT 恢复 |
| Active Data append/commit | `segment.before-append-write` | `segment.before-sync` | N/A | N/A | 已覆盖；write 错误 poison Active，Seal 已开始后的 sync 错误返回 CommitUnknown，Store fail closed |
| Active Data seal/rotation | seal/footer write 已覆盖 | seal/footer sync 已覆盖 | seal rename 已覆盖 | data dir sync 已覆盖 | 已覆盖已有 Active 的 seal 主路径；每个失败点均可由 fresh recovery 得到严格 sealed 文件 |
| Active Data create/tail repair | 未覆盖 | 未覆盖 | N/A | 未覆盖 | 待补；包括新 Active Header、repair truncate/fsync |
| Rotation Journal | 已覆盖 | 已覆盖 | 已覆盖 install/remove | 已覆盖 install/remove | 完整 syscall matrix；Phase 1–5 publication 均验证，未发布 temp 在 Open 时按 regular-file 规则删除并 fsync，已发布 Journal 幂等完成 rotation |
| Maintenance Journal | 已覆盖 | 已覆盖 | 已覆盖 install/remove | 已覆盖 install/remove | 完整 syscall matrix；checkpoint 前清理失败会 fail closed，checkpoint publication 与 phase-4 Journal 之间失败由 Manifest 证明并在 Open 时继续 Data GC |
| Active Mapping append/checkpoint | `mapping.before-append-write` | `mapping.before-sync` | N/A | N/A | 已覆盖；任一错误 poison Active Mapping writer，Store fail closed，fresh Open 从旧 Manifest + Log replay 重建 |
| Active Mapping open tail repair | N/A | `mapping.before-tail-sync` | truncate 已覆盖 | N/A | 已覆盖；truncate 前完整遍历 durable Root，引用损坏 tail 时拒绝 Open 且不修改文件；失败 Open 可重试 |
| Mapping rotation | Footer/Header write 已覆盖 | Active/Footer/Header sync 已覆盖 | seal rename、recovery truncate/remove 已覆盖 | mapping dir sync 已覆盖 | 完整 runtime/recovery matrix；普通与 Data GC nested rotation 均恢复，Journal hook 同源传播；恢复失败可重试并补做已存在 sealed/new Active 的 file/dir sync |
| Mapping GC | Header/Node/Footer 已覆盖 | sealed/final Active sync 已覆盖 | temp publish、checkpoint 前 cleanup、old-file trash/delete 已覆盖 | temp/publish/mapping/trash/delete dir sync 已覆盖 | 完整 runtime/recovery matrix；三类错误、部分多文件操作、Manifest publication 不确定性和 fresh Open 收敛均验证 |
| Data GC trash/delete | N/A | N/A | 未覆盖 | 未覆盖 | 待补；必须分别证明删除前/后的 Manifest 与 Journal 收敛 |
| Initialize marker/files | 未覆盖 | 未覆盖 | 未覆盖 | 未覆盖 | 待补；已有初始化 crash matrix |
| Backup/Restore artifact publication | 未覆盖 | 未覆盖 | 未覆盖 | 未覆盖 | 待补；不影响已打开 Store，但影响可声明的离线运维完整性 |

## Active Data 已闭合的失败语义

- append `WriteAt` 返回错误：`ActiveData` 立即 poisoned，Append Log/Store fail closed；不得继续分配 FrameSeq 或把该 Batch 判为成功；
- CommitSeal 已完整写入后 sync 返回错误：调用者得到 `ErrCommitUnknown` 并保留底层 cause；fresh Open 以实际完整 Frame/CRC 判定 Committed 或 Aborted；
- Seal/Footer write 或 sync 返回错误：当前 Active poisoned，Rotation Journal 保留；fresh recovery 识别完整 terminal Seal 或截断不完整尾部；
- rename 或 data directory sync 返回错误：运行时不猜测文件名是否 durable；fresh recovery 同时识别合法 `.active` 或 sealed name 并完成转换；
- 任一注入点后均不得继续前台写。Segment 单元矩阵对每一点分别注入 `EIO/ENOSPC/EACCES` 并验证 cause 与恢复；Store 级 descriptor write/sync Case 使用 `EIO` 再验证对外 Aborted/CommitUnknown 和 fail-closed 语义。

## 剩余推进顺序

1. Data GC trash/delete；
2. Active Data create/tail repair；
3. Initialize 与 Backup/Restore。

只有所有行闭合，`phase-5-audit.md` 的“所有 durable writer 完成 syscall error matrix”才能勾选。

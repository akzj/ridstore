# 72h Steady-State Soak

状态：harness 已实现；72 小时自然运行证据未完成。

`ridstore-soak` 在一个必须不存在的新目录中先建立有界 stable-ID 工作集，再从 seed
完成时开始计算完整 duration，持续执行
Put/overwrite/delete/Abort、durable Commit、Checkpoint、Data GC 和 Mapping GC。
工作负载停止后，它排空可回收 Data Segment、Compact Mapping、逐 ID 对照内存模型，
关闭 Store 并执行 offline Verify。

```text
SOAK_DIR=/dedicated-disk/ridstore-soak-$(date +%s) \
SOAK_REPORT=/reports/ridstore-soak-$(date +%s).jsonl \
make soak-72h
```

两个路径都必须不存在；工具使用 exclusive create，绝不覆盖既有 Store 或报告。
收到 SIGINT/SIGTERM 会通过 context 停止、写出 terminal failure record 并返回失败，保留此前 JSONL 样本，但不能算
自然完成。开发机短 smoke 只验证 harness：

```text
make test-soak-smoke
```

维护在 interval 或累计 committed batches 任一门限到达时执行，避免前台速度高时积压到
结束阶段。最终 Data GC 以 allocated bytes 是否继续下降判断 quiescence；GC 自身会追加
Relocation/Checkpoint 记录，因此不能错误地把“候选数必须为零”当作唯一收敛条件。

每个 sample 保存 commit/GC/Delta/Mapping Cache metrics、Mapping reachable/unreachable、
Data/Mapping active/sealed 文件数、trash/tmp、Manifest/Stats cut、logical/allocated disk、
RSS、FD 和 goroutine。最终 summary 只有在指定 duration 自然结束、维护收敛、逐记录
最终维护完成、逐记录模型一致、offline Verify clean，且 FD/goroutine 回到基线附近时才写出
`completed_naturally=true` 与 `verified_clean=true`。

空间收敛不能由单个 summary 布尔值替代：Review 必须检查整个 JSONL 中 steady-state
allocated/logical bytes 的包络、GC copied/reclaimed、sealed/trash 数量与结束阶段样本，
确认它们没有随时间无界增长。

有效的 72h 报告还必须保存 Git commit、命令参数、Go/kernel/filesystem/device 信息；
harness 的成功短测、被信号终止的长测或手工截取的稳定区间都不能替代自然结束报告。

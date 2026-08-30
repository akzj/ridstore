# v2 Durable Benchmark Harness

状态：ridstore 与 raw append 基线已实现；跨引擎对比未完成

## 1. 当前负载

根包 benchmark 使用公开 v2 API，覆盖 128B、4KiB 和 64KiB value：

- `BenchmarkDurableCreate`：每个操作 Begin/Create/Commit 一个新 Stable ID；
- `BenchmarkDurableHotOverwrite`：并发 blind overwrite 同一 Stable ID；
- `BenchmarkDurableMaintenanceInterference`：对同一份预热 Mapping 比较无维护、连续
  Checkpoint、连续 Mapping Compact 三种场景；每个前台操作执行 Get、Put、Commit，
  分别报告 p50/p99/p999/max 和已完成维护次数；该 workload 使用 1MiB Segment，令持续
  overwrite 在测量期间自然触发 Data rotation，同时覆盖 Catalog generation rebase；
- `BenchmarkDurableAppendBaseline`：向同一 regular file append 一个 value 并每次 `fsync`。

raw append 只是设备和文件系统的 durable lower bound。它没有 framing、recovery、Mapping、
Stable ID 或原子 Batch，因此不能被描述为与 ridstore 功能等价。并发度由 Go benchmark
`-cpu` 控制，用于观察 group commit，不将默认本机结果固定为容量承诺。

维护干扰场景故意不在两次维护之间 sleep，它表达的是最坏情况下的 back-to-back
运维压力，不是推荐的生产调度。Checkpoint 的 durable marker 与 Commit 共用 WAL/fsync
排序点，短暂阻止新 Commit admission；Put 虽不经过 checkpoint admission fence，仍可能在
单 append writer 正执行 marker fsync 时排队。因此结果必须同时看维护频率和尾延迟，不能把
“不存在 Store 全局锁”误读为“维护没有共享 I/O”。生产调度仍应在高峰降低调用频率，并为
连续维护设置间隔或 I/O 预算。

RecordLog rotation 的同步窗口仍会暂停单 append writer，但 journal durable 之后，旧 Segment
footer seal 与新 Active header 创建会并行执行；Catalog 只在两侧都 durable 后发布。这个优化
缩短 rotation 窗口，不改变 crash recovery 的完成顺序。

## 2. 可重复产物

```text
make bench BENCH_REPORT_DIR=/new/report/dir BENCH_TIME=3s BENCH_COUNT=3
```

report 目录必须不存在。runner 保存 Git commit/dirty 状态、Go/OS/CPU 目标、kernel、
filesystem 与容量信息到 `metadata.txt`，保存原始 Go benchmark 输出到 `benchmark.txt`，
并且只在全部正常结束时发布 `COMPLETED`；中断或失败留下 `FAILED`。

dirty tree 仍可用于开发期调试，但 `git_dirty=true` 的产物不得作为 release 证据。

## 3. 仍未完成

- Pebble/RocksDB 在相同 fsync/WAL、batch、value size、concurrency 和稳态 compaction 条件下的 adapter；
- random/conditional/delete/mixed/read/checkpoint/GC workload matrix；
- CPU、RSS、FD、space amplification、recovery 与独立侧车采集；
- paced Checkpoint/Data Compact 与真实日夜调度曲线；
- 固定机型和文件系统上的多轮稳态原始报告。

因此当前完成的是 benchmark 入口和第一个 durable 下界，不是
same-durability 跨引擎结论。

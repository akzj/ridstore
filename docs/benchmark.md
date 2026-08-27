# v2 Durable Benchmark Harness

状态：ridstore 与 raw append 基线已实现；跨引擎对比未完成

## 1. 当前负载

根包 benchmark 使用公开 v2 API，覆盖 128B、4KiB 和 64KiB value：

- `BenchmarkDurableCreate`：每个操作 Begin/Create/Commit 一个新 Stable ID；
- `BenchmarkDurableHotOverwrite`：并发 blind overwrite 同一 Stable ID；
- `BenchmarkDurableAppendBaseline`：向同一 regular file append 一个 value 并每次 `fsync`。

raw append 只是设备和文件系统的 durable lower bound。它没有 framing、recovery、Mapping、
Stable ID 或原子 Batch，因此不能被描述为与 ridstore 功能等价。并发度由 Go benchmark
`-cpu` 控制，用于观察 group commit，不将默认本机结果固定为容量承诺。

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
- CPU、RSS、FD、space amplification、recovery 与 maintenance tail latency 侧车采集；
- 固定机型和文件系统上的多轮稳态原始报告。

因此当前完成的是 benchmark 入口和第一个 durable 下界，不是
same-durability 跨引擎结论。

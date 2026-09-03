# Metrics Export

状态：v2 operational contract

## 1. 边界

`Store.Metrics()` 只读取有界的 atomic/runtime 状态和 Mapping Delta 计数，不执行
磁盘 I/O。快照用于调度、容量观察和诊断，不是事务一致 snapshot，也不能授权
Checkpoint、GC candidate 或文件删除。

`Metrics.AppendMetricSamples(dst)` 把快照转换为固定 74 个稳定 sample。调用方提供
足够容量的 slice 时不分配。Counter 是当前进程生命周期累计值，进程重启后归零；
Gauge 是采样时刻的内存值。所有 latency/duration 使用整数 nanoseconds，避免在
内核层损失精度；外部 backend 可转换为 seconds。

## 2. Prometheus adapter

项目提供无第三方依赖的 `metrics/prometheus` HTTP handler：

```go
handler, err := prometheus.NewHandler(store, map[string]string{
    "service": "example",
    "shard":   "01",
})
http.Handle("/metrics/ridstore", handler)
```

handler 每次 scrape 读取一个新快照，输出 Prometheus text format 0.0.4。constant
labels 在构造时验证、排序和转义；应用不得使用文件路径、Record ID 等无界高基数
值。一个进程有多个 Store 时，由应用显式提供稳定的 `store`/`shard` label，框架
不会自动暴露本机路径。

adapter 不启动 goroutine、不注册全局 collector、不拥有 HTTP server，也不把
Prometheus SDK 引入 ridstore 内核。OpenTelemetry 或自有 exporter 可直接消费
`MetricSample`。

## 3. 稳定名称

- Commit counters：`ridstore_commit_*_total`、`ridstore_committed_total`、
  `ridstore_aborted_total`、`ridstore_conflicts_total`；
- 分段耗时：`ridstore_*_nanoseconds_total`；
- Checkpoint：fence 次数、获取/持有累计耗时、最大持有耗时，以及完整 Checkpoint
  started/completed/failed、累计/最大耗时；
- RecordLog rotation：次数、累计耗时与最大单次耗时；
- Mapping GC：started/completed/failed/conflicts、累计/最大耗时，以及 rebuild/verify/publish 阶段累计耗时和最大 publish 持锁时间；并发 Checkpoint 使 Root 前进属于 conflict，不计为故障；
- Delta/Cache gauges：`ridstore_delta_*_bytes`、`ridstore_mapping_cache_bytes`；
- space admission：`ridstore_disk_available_estimate_bytes`、`ridstore_write_stop_free_bytes`、
  `ridstore_write_stopped`、拒绝和检查错误 counter；
- GC counters：`ridstore_gc_*_total`，copied/reclaimed 使用 bytes，relocated/skipped
  使用 records；另导出 throttled nanoseconds、Data/Mapping GC space rejection、
  `gc_min_free_bytes` 和当前 `gc_bytes_per_second`。Open Batch redirect 额外报告 redirect
  次数、顺序流等待/admission boundary nanoseconds 和实际重定向 ref 数；wait 表示 redirect install
  在 Coordinator 顺序流中的等待，admission 表示 removal boundary 短暂关闭 admission 的时间。
- 后台 Checkpoint counters：`ridstore_background_checkpoint_requested_total`、
  `ridstore_background_checkpoint_completed_total`、`ridstore_background_checkpoint_failed_total`。
  requested 统计被调度器接受的不同 Delta pressure generation；同代重复通知以及已被其他成功
  Checkpoint 覆盖的迟到通知不会重复计数。
- Scheduler：requested/coalesced/completed/failed/preemptions counter 与 queued/running gauge；Mapping survey
  额外导出 generation、physical bytes 和 reachable bytes，只有 generation 复核成功的结果才更新。
Commit pipeline 数据直接来自 v2 Coordinator；终态计数由 v2 Batch 生命周期产生；Delta、Mapping cache、
space admission 是即时 gauge；Data GC copied/reclaimed 使用物理 Record bytes。GC throttle 和独立
space admission 指标来自真实 pacing 与两阶段准入结果。

名称、kind 和 base unit 属于 public adapter contract。新增 metric 可以 append，
已有名称不能静默改义；删除或改名需要版本化迁移。

## 4. 已知限制

- 多个 atomic 字段不是同一时钟点的线性一致 snapshot；
- 当前提供累计耗时，并为 Checkpoint fence、Checkpoint、RecordLog rotation 和 Mapping GC
  提供进程内 max；没有 histogram/quantile，p99/p999 仍应由调用路径 tracing 或外部
  histogram 采集，不能从累计值或 max 反推；
- Mapping physical/reachable bytes 是最近一次异步 survey 的 generation-bound 缓存，不保证与本次
  `Metrics()` 调用时的最新 Manifest 同代；Scrub 结果仍属于显式维护查询。
- `disk_available_estimate_bytes` 是最近 `statfs` 结果减去本进程已 admission 的
  payload/reserve 估计；它可能尚未观察外部写入及 Commit/Checkpoint/GC，不能当作配额。

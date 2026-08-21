# Metrics Export

状态：Phase 5 operational adapter v1

## 1. 边界

`Store.Metrics()` 只读取有界的 atomic/runtime 状态和 Mapping Delta 计数，不执行
磁盘 I/O。快照用于调度、容量观察和诊断，不是事务一致 snapshot，也不能授权
Checkpoint、GC candidate 或文件删除。

`Metrics.AppendMetricSamples(dst)` 把快照转换为固定 27 个稳定 sample。调用方提供
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
- Delta/Cache gauges：`ridstore_delta_*_bytes`、`ridstore_mapping_cache_bytes`；
- GC counters：`ridstore_gc_*_total`，copied/reclaimed 使用 bytes，relocated/skipped
  使用 records。

名称、kind 和 base unit 属于 public adapter contract。新增 metric 可以 append，
已有名称不能静默改义；删除或改名需要版本化迁移。

## 4. 已知限制

- 多个 atomic 字段不是同一时钟点的线性一致 snapshot；
- 当前只有累计耗时，没有内核内 histogram/quantile；p99/p999 应由调用路径 tracing
  或外部 histogram 采集，不能从累计值反推；
- Metrics 不包含需要磁盘遍历的 Mapping reachable bytes 或 Scrub 结果；这些属于
  显式维护查询。

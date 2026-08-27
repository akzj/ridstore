# Long Fuzz 与 Nightly Evidence

状态：v2 runner/workflow 已实现；自然结束的 long-fuzz 证据尚未完成。

短时 `make test-fuzz-smoke` 是每次提交的 decoder 回归门禁，不是长期证据。长时入口对
9 个 v2 不可信 decoder target 逐个运行独立 `go test -fuzz`，每个 target 默认
30 分钟，总时长约 4.5 小时：

```text
mkdir -p /reports/ridstore
make test-fuzz-long \
  FUZZ_REPORT_DIR=/reports/ridstore/long-fuzz-$(date +%s) \
  FUZZ_LONG_TIME=30m \
  FUZZ_PARALLEL=4
```

`FUZZ_REPORT_DIR` 必须不存在且父目录必须已存在。runner 生成环境/Git 元数据、每 target
原始日志、TSV summary、发现的 Go fuzz corpus，以及唯一 terminal marker：全部自然成功
才写 `COMPLETED`；失败、中断或信号退出写 `FAILED`。已有报告绝不覆盖。

`.github/workflows/nightly-fuzz.yml` 每日及手动执行同一入口，Action 固定到具体 commit，
总 timeout 为 5 小时，原始报告保留 30 天。发布证据必须把选定成功 artifact 归档到发布
记录；GitHub 的滚动 retention 不是永久审计存储。

有效证据要求：9 个 target 全部自然结束、`COMPLETED` 存在、`FAILED` 不存在、summary
全为零退出码、`git_dirty=false`，并保留对应 commit 和原始日志。`test-fuzz-harness-smoke`
只以单 target/1 秒验证 runner 的目录、报告和 terminal 状态机，不能关闭 long-fuzz 门禁。

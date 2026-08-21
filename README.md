# ridstore

ridstore 是一个嵌入式、单机的 Stable-ID Log-Structured Record Store Library。

它提供一个很小的存储抽象：

```text
uint64 ID -> variable-length bytes
```

逻辑 ID 稳定且永不复用，物理记录只追加写入。更新产生新的物理记录，删除只改变可见映射，旧记录由后台 GC 回收。多个 Put/Delete 可以组成一个原子 Batch，并通过一次或合并后的 fsync 持久化。

默认并发覆盖按 CommitSeq Last-Writer-Wins；需要防止丢失更新时，可使用 LogicalRevision 和 Batch 级 `ExpectRevision`/`ExpectAbsent` 做显式乐观冲突检查。

ridstore 只负责 Record，不内置 KV、Page、Blob、Stream、SQL 或业务保留策略。这些能力应当构建在稳定 ID 和原子 Batch 之上。

当前 Phase 0–4 已完成并通过阶段 Review，正在进入 Phase 5 完整性与运维；项目仍未达到 production-ready：

- [设计文档索引](docs/README.md)
- [总体设计](docs/design.md)
- [与 LSM/RocksDB 的定位边界](docs/positioning-vs-lsm.md)
- [Library API 契约](docs/api-contract.md)
- [磁盘格式](docs/on-disk-format.md)
- [Commit/Recovery](docs/commit-recovery-protocol.md)
- [Mapping](docs/mapping-design.md)
- [GC](docs/gc-protocol.md)
- [SegmentStats](docs/segment-stats-design.md)
- [配置与 Backpressure](docs/runtime-config.md)
- [验证计划](docs/verification-plan.md)
- [实施计划](docs/implementation-plan.md)

ridstore 类似 RocksDB 的交付形态，是由应用进程直接链接并独占本地数据目录的 storage engine；但它不以替代 RocksDB、LevelDB 或其他通用有序 KV 引擎为目标。独立进程、RPC、复制和分布式系统不属于当前范围。

# ridstore

ridstore 是一个 Stable-ID Log-Structured Record Store。

它提供一个很小的存储抽象：

```text
uint64 ID -> variable-length bytes
```

逻辑 ID 稳定且永不复用，物理记录只追加写入。更新产生新的物理记录，删除只改变可见映射，旧记录由后台 GC 回收。多个 Put/Delete 可以组成一个原子 Batch，并通过一次或合并后的 fsync 持久化。

ridstore 只负责 Record，不内置 KV、Page、Blob、Stream、SQL 或业务保留策略。这些能力应当构建在稳定 ID 和原子 Batch 之上。

当前项目处于设计阶段，尚未开始实现：

- [总体设计](docs/design.md)
- [与 LSM/RocksDB 的定位边界](docs/positioning-vs-lsm.md)

ridstore 不以替代 RocksDB、LevelDB 或其他通用有序 KV 引擎为目标。

# v2 Record Metadata Cache

状态：Implemented performance cache

## 1. 边界

`VAddrMetaCache` 是进程内、有界、可丢失的 Put metadata 缓存：

```text
VAddr -> (RecordID, PhysicalSize)
```

它不是 Mapping、Recovery、Verify 或 GC 删除的权威证据。重启后缓存为空；缺失或淘汰只能
导致回退 `RecordLog.Inspect`，不能改变结果。Offline Verify 始终禁用该缓存。

## 2. 填充和消费

- Put append 成功后，由 Engine appender 以已编码的 Put header 和 physical size 填充；
- Get 完成 Mapping 二次确认和 Put decode 后填充；
- Relocation copy append 成功后填充新 VAddr；
- SegmentStats 只查询，不把全量扫描 miss 写回，避免顺序扫描污染有界缓存。

命中项仍校验 `RecordID`、VAddr size tag 和 Segment boundary。miss 保留现有 32-byte
Record Header + 32-byte Put Header 读取与验证。Record immutable，因此同一 VAddr 的 metadata
不存在正常更新；逻辑 overwrite/delete 也不要求立即删除旧 cache entry。

## 3. 容量与实现

`RuntimeConfig.RecordMetaCacheEntries` 指定近似最大条目数，零值默认 65,536。实现使用
64 分片、4-way set-associative 固定槽位；没有 per-entry allocation 或 LRU 链表。容量按 4 个槽位
向上取整，所以小配置最多比请求值多 3 个槽位。直接碰撞替换只影响命中率。

公开 metrics 追加：

- `ridstore_record_meta_cache_hits_total`；
- `ridstore_record_meta_cache_misses_total`；
- `ridstore_record_meta_cache_entries`；
- `ridstore_record_meta_cache_evictions_total`。

## 4. 与增量 SegmentStats 的关系

当两次 Checkpoint 之间 Data active segment 未轮转时，SegmentStats 以上一代精确
stats 为基线，只处理 frozen layers 折叠后的 changed IDs。新旧 VAddr 位于 sealed
segment 时才需要 metadata；active segment 不输出 stats，因此不读 Header。

如果 active segment 已轮转，上一代被省略的 active 已变成 sealed。此时只顺序扫描
这一个 former-active segment，并与 candidate Mapping join 得到它的精确值；转动后的其他
segment 由 folded changes 完整覆盖。只有基线 Mapping/Data topology 无法证明匹配时才回退
全量 Root walk。

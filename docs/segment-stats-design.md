# SegmentStats 设计

状态：Implemented hybrid incremental builder

## 1. 定位

SegmentStats 用于 GC 候选选择、容量观测和空间收敛判断，不参与 Record 可见性，也不能单独授权删除 Segment。

正确性边界：

```text
Mapping 决定 Record 是否 live
SegmentStats 只提供保守候选提示
GC 删除前必须重新执行精确 Mapping 校验
```

Put/Commit 热路径不能为了更新统计而读取旧 Mapping Record Header。统计的精确基线由 Mapping Checkpoint 后台批量生成。

## 2. 统计状态

每个 Data Segment 的 checkpoint 统计：

```text
SegmentID
ExactLiveBytesAtCut
ExactLiveRecordsAtCut
```

全局字段：

```text
StatsCoveredCommitSeq
```

它必须与同一 Manifest 中的 Mapping Root `CoveredCommitSeq` 相等。表中缺失的 Segment 在该 cut 的 live bytes/count 均为 0；表只保存非零项并按 SegmentID 升序排列。

运行时另维护：

```text
PhysicalBytes          // 来自 append position/Footer，精确
PutRecordBytes         // 来自 Frame scan/append，精确
LiveUpperBytes         // 基线 + cut 后成功发布的新 Record bytes
LiveUpperRecords
KnownDeadBytes         // 可选提示，不参与删除证明
StatsBaseCommitSeq
```

`LiveUpperBytes` 是当前 live bytes 的保守上界：cut 后每个成功映射到 NewVAddr 的用户 Put 或成功 Relocation 都累加新 Record 大小，但不在前台同步扣减 old VAddr。Delete 不累加。这样 Blind Commit 不需要读取旧 Record，重复覆盖只会高估，不会低估。

## 3. Checkpoint 批量统计

Mapping Checkpoint barrier 得到 `(C, F, ReplayStart)` 并冻结 Overlay 后，Builder 同时构建新 Root 和 SegmentStats(C)：

```text
base Mapping Root R0 at C0
base SegmentStats at C0
+ all frozen final mutations through C
=> new Mapping Root R1 at C
=> exact SegmentStats at C
```

处理步骤：

1. 将所有 frozen layer 从旧到新折叠，每个 ID 只保留 cut 时最终 mutation；
2. 从 base Root 查出该 ID 的 OldVAddr；
3. 根据最终 mutation 得到 NewVAddr 或 NotFound；
4. OldVAddr==NewVAddr 时跳过；
5. 对 sealed Old/New VAddr 优先查 metadata cache，miss 时读取并校验 Record/Put Header；
6. active 转动时顺序扫描 former-active，以 candidate Mapping 精确 join；
7. 从 metadata 或扫描边界获得物理 Record bytes；
8. 从 old Segment 的 live bytes/count 扣减，从 new Segment 增加；
9. 删除归零的表项，按 SegmentID 排序编码；
10. 与新 Mapping Root 一起写入同一 Manifest generation。

当前实现执行上述 base-to-final 差分，不遍历未变的 Mapping ID。如果 active 已转动，
上一代 stats 中被省略的 active segment 通过单 Segment 顺序扫描与 candidate Mapping join
重建；转动后新建 Segment 中的 live 记录完全来自 frozen changes。只有基线拓扑无法证明
匹配时才保留全量 candidate Root 回退。

同一 ID 在两个 checkpoint 之间被覆盖多次，只统计：

```text
base-root OldVAddr -> cut-time final NewVAddr
```

中间版本、Abort Record、冲突 Batch Record 和失败 Relocation 副本从未进入新 Root，因此自然不计入 live stats。

统计扣减发生 underflow、VAddr 指向错误 RecordID、Header/CRC 错误或 Segment 身份冲突都表示 corruption/实现错误；Checkpoint 不得安装新 Root。

## 4. 原子安装

Checkpoint Manifest 原子安装以下同一 cut 状态：

```text
MappingRootAddr
CoveredCommitSeq = C
CutFrameSeq = F
ReplayStartLogPos
StatsCoveredCommitSeq = C
SegmentStatsTable(C)
```

不能先安装 Root、以后再补同一 C 的统计，也不能把旧 Stats 表标成新 CoveredCommitSeq。Manifest 发布前崩溃继续使用旧 Root+旧 Stats；发布后同时使用新 Root+新 Stats。

统计构建属于后台 Checkpoint 工作，不持有 `publishMu`。只有 cut 指针交换和最终 immutable state 切换使用短锁。

## 5. Commit 与 Recovery 增量

Checkpoint cut 后，前台 Mapping Publish 对成功产生 NewVAddr 的 mutation执行：

```text
LiveUpperBytes[new.segment] += new.TotalSize
LiveUpperRecords[new.segment]++
```

不要求查找或扣减 old VAddr。增量不是一个会在 checkpoint 时整体清零的全局 counter，而是附着于 active/frozen Delta layer：

```text
LiveUpper = exact Stats Base
          + active Delta stats additions
          + every frozen Delta stats additions
```

Mutation 与当前 active Delta 的 additions 在同一次 Mapping Publish 临界区更新。Checkpoint 安装新 Base 时只移除已被精确统计吸收的 merged layers；cut 后的 layer 仍保留，因此并发 Commit 的增量不会在 Base 切换时丢失。它仍是派生状态；崩溃后由 Manifest Base + replay 重建，不影响 Mapping 正确性。

Open/Recovery 加载 exact Stats(C)，随后重放 ReplayStart 后的 Commit/Relocation：

- 用户 Put：累加 NewVAddr；
- Delete：不累加；
- Relocation CAS 成功：累加 NewVAddr；
- Relocation CAS 失败：不累加；
- Abort/无 Seal Record：不累加。

Recovery 本来就必须验证被提交 NewVAddr 的 PutRecord，因此可以使用已读取 Header.TotalSize，不增加旧 Record 随机读取。

下一次 Mapping Checkpoint 重新生成 exact Stats，并消除上界中的重复累加。

## 6. GC 使用规则

候选估算：

```text
reclaimableLowerBound = max(0, reclaimablePhysicalBytes - LiveUpperBytes)
liveRatioUpperBound   = LiveUpperBytes / reclaimablePhysicalBytes
```

Stats 可以决定扫描优先级，不能跳过以下删除门禁：

1. 精确扫描源 Segment；
2. 对每个 PutRecord 验证 `Mapping[ID] == VAddr`；
3. Relocation 后再次确认无 Mapping 指向源 Segment；
4. Mapping Checkpoint 覆盖 Relocation；
5. reader/open-batch/maintenance pin 为 0；
6. Manifest 移除和 rename-to-trash durable。

即使 Stats 显示 live=0，也不能直接删除。

v2 的自动候选路径在选择前先做 Checkpoint，并只选择完整落在 ReplayStart 之前的 immutable sealed
Segment。由于用户 Put 只能 append 到 active Segment，且 relocation 由全局 maintenance gate 串行，
Checkpoint 后不会新增指向该候选 source 的 Mapping；该 source 的 exact stats 因而可直接作为选择阶段
的 live upper bound。这个约束替代了为当前 v2 热路径维护全局 `LiveUpper` 镜像状态，但不改变删除前的
精确证明要求。

## 7. 资源与 Backpressure

- SegmentStats 表项数量受当前含 live Record 的 Segment 数限制；
- Header 读取按 Segment/offset 排序并使用有界 buffer；
- Builder 不一次性加载 Value payload；
- Stats 内存计入 Checkpoint memory budget；
- Checkpoint 失败保留旧 Root、旧 Stats 和 frozen Delta；
- stats build 速度追不上时由总 Overlay hard limit 对新 Commit backpressure，不能丢弃统计或 Delta。

## 8. 验收

- 连续 create、overwrite、delete 后 Stats(C) 与全量 Mapping 遍历一致；
- 同 ID 多次覆盖只计算 base→final；
- Abort、冲突和 CAS 失败副本不计 live；
- cut 后 LiveUpper 从不低于精确 live；
- Recovery replay 后 LiveUpper 仍为上界；
- Stats/Root Manifest 崩溃点只能同时选择旧代或新代；
- Stats=0 不能绕过 GC 精确校验；
- 长期 checkpoint/GC 后 physical/live/dead 指标收敛。

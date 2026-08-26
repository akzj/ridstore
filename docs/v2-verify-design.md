# ridstore v2 Offline Verify

状态：Design frozen for implementation

## 1. 定位

v2 Verify 是独立的只读审计路径，不是 `Open` 的一个选项，也不是 repair：

```text
acquire exclusive LOCK
  -> reject recovery artifacts
  -> load authoritative Manifest
  -> verify physical live file set
  -> verify Mapping reachability
  -> replay durable tail into a verifier-owned logical view
  -> join final ID -> VAddr with Put Records and SegmentStats
  -> return bounded report
```

它不得调用会 truncate active tail、完成 rotation、清理 temp/trash 或安装 Manifest 的正常恢复入口。
发现可由正常 Open 收敛的状态时返回 `ErrRecoveryRequired`；发现已发布事实自相矛盾时返回
`ErrCorrupt`。Verify 永远不修改被检查目录。

## 2. 所有权

每层只验证自己拥有的格式，不在顶层复制 decoder：

| owner | 只读验证职责 |
|---|---|
| bootstrap / maintstate | INITIALIZING、MAINTENANCE 与临时 marker 是否存在 |
| storecatalog | 两个 Manifest slot 的 CRC、版本、全局字段约束和最高 generation 选择 |
| recordlog | Catalog 指定的 Data Segment header/footer、完整 Record、active tail 和精确文件集合 |
| mapstore | Catalog 指定的 Mapping Segment、Node 序列、active tail、精确文件集合和按地址读取 |
| radix | 从 Manifest Root 遍历可达树，验证 level/prefix/child identity，输出有序 `ID -> VAddr` |
| replay | 从 ReplayStart 验证 Record protocol、CommitSeq、allocator high 和 Batch 状态机 |
| verifier | 组合各层事实，校验 final Mapping、Put identity、地址唯一性和 SegmentStats |

RecordLog 和 MapStore 可以增加只读 inspector，但不能为了 Verify 引入第二套 writer、Catalog 或恢复状态机。

## 3. 一致性输入

Verify 必须先取得与正常 Store 相同的目录独占锁。锁成功后，目录不再有合法 writer，Manifest 与文件集
形成稳定输入。锁失败返回 `ErrLocked`，不能退化成无锁扫描。

以下任一 artifact 存在都返回 `ErrRecoveryRequired`：

- `INITIALIZING-v2` 或其 temp；
- RecordLog rotation journal 或 temp；
- Mapping rotation journal 或 temp；
- `MAINTENANCE.v2` 或其 temp；
- GC trash 或 protocol 已知的 creating/temp 文件。

未知文件不自动删除或接纳。若它位于受管目录并可能冒充 Segment，则报告 corruption；明确的协议临时文件
报告 recovery-required。分类只影响诊断，不触发写操作。

## 4. 物理验证

### 4.1 Data Segment

- sealed 文件大小必须精确等于 `ValidEnd + FooterSize`；
- Header identity、Footer summary 与 Manifest 必须相同；
- 从 Header 到 ValidEnd 顺序解码全部 Record envelope 和完整 payload CRC；
- active 文件必须是从 Header 开始的完整 Record 序列；末尾半条 Record 返回
  `ErrRecoveryRequired`，中间坏 CRC、坏地址或非法边界返回 `ErrCorrupt`；
- Record VAddr 必须由 `(SegmentID, offset, physical size)` 唯一重建。

### 4.2 Mapping Segment

- sealed 文件、Header、Footer、NodeSeq 与 Manifest summary 精确一致；
- active 文件从 Header 到 EOF 必须是完整 Node 序列；可截断 tail 返回 `ErrRecoveryRequired`；
- Node decoder 验证 CRC、encoding、prefix、slot 和 CoveredCommitSeq；
- Manifest Root 必须位于 live file set 内的完整 Node 边界。

## 5. 逻辑验证

Verifier 从 checkpoint Root 得到 base Mapping，再从 Manifest ReplayStart 重放 tail。它复用正式 replay
协议，但输出写入 verifier-owned overlay，不启动运行时 writer。最终视图必须满足：

1. 每个 ID 只出现一次且非零；
2. 每个 VAddr 只被一个 ID 引用；
3. VAddr 指向 Put Record，且 Put.RecordID 等于 Mapping ID；
4. Commit/Relocation Descriptor 引用的 Put identity、OriginBatchID 和地址关系合法；
5. CommitSeq 从 `CoveredCommitSeq + 1` 连续；
6. allocator high 不回退，开放 Batch 恢复结果符合正式 Replay；
7. 从 checkpoint Root 计算的 exact live bytes/records 与同代 SegmentStats 相同。

Value 只在校验当前 Put 时读取并立即释放；第一版允许保存 `O(live IDs)` 的 ID/VAddr join 状态，不保存
全部 Value。资源上限必须来自显式 Verify 配置，不能借用运行时 cache 或 Delta budget 后静默失真。

## 6. API 与报告

第一版嵌入式入口：

```go
Verify(ctx context.Context, config VerifyConfig) (VerifyReport, error)
```

`VerifyConfig` 只包含目录和 verifier 自己的内存/issue 上限，不接受 HardLimits；磁盘 Manifest 是格式事实。
`VerifyReport` 至少包含 Manifest generation、扫描的 Data/Mapping 文件与字节、Record/Node 数、live ID 数、
replayed commit 数和完成阶段。`error == nil` 才表示 clean；报告即使失败也只包含失败前已证明的统计。

首版 fail-fast，保留结构化 stage；后续若增加多 issue 收集，遇到不可信 length/count 后仍必须停止该文件，
不能为了多报错误继续越界解析。

## 7. 实现阶段

1. **M1 physical inspector（已实现）**：锁、artifact 门禁、Manifest、Data/Mapping 全文件只读扫描；
2. **M2 reachable Mapping**：Radix 全遍历、地址/层级/前缀与 alias 校验；
3. **M3 semantic replay**：从 cut 重放并形成 final verifier view；
4. **M4 exact join**：Put identity、最终地址唯一性、SegmentStats 精确比较；
5. **M5 public/report**：公开 API、corruption/process-exit tests、CLI 可选封装。

每个阶段只能声称它实际证明的范围。M1 通过不能称为 Store clean；在 M4 完成以前 API 不对外发布。

## 8. 不做的事情

- 不调用正常 `Open` 后再声称是离线审计；
- 不自动 truncate、删除 orphan、完成 journal 或刷新 checksum；
- 不通过目录扫描推举未被 Manifest 引用的文件；
- 不恢复 v1 verifier、Revision 或旧格式兼容分支；
- 不让 Verify report 成为 GC 删除授权或运行时状态来源。

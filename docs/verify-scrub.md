# Offline Verify 与 Scrub Report

状态：Phase 5 implementation contract v1

## 1. 命令

```text
go run ./cmd/ridstore-tool verify \
  --dir /path/to/store \
  --mapping-cache-bytes 268435456 \
  --max-live-ids 1048576 \
  --status-limit 65536
```

后三个资源上限可以省略并使用上面的默认值，但显式传入 0 非法，不表示无界。

退出码：

- `0`：报告 `clean=true`；
- `1`：检测到 corruption、需要先恢复、目录被占用或 I/O 失败；stdout 仍尽可能输出 JSON report，诊断写入 stderr；
- `2`：命令参数错误。

stdout 固定输出 `{ "clean": bool, "report": ... }`。只有无错误到达 `exact-join` 才有
`clean=true`；失败仍输出截至失败 Stage 已证明的部分 report，原始错误写入 stderr。

Verify 必须离线运行并取得 Store 的独占 `LOCK`。它不会调用正常 `ridstore.Open`，因为 Open 可以完成 Journal、截断 invalid active tail 或安装恢复状态，这些写操作会污染只读审计证据。

## 2. 只读边界

Verify：

- 只通过 CURRENT 加载已发布 Manifest；
- 发现 INITIALIZING、MAINTENANCE、ROTATION 或对应 temp artifact 时返回 `ErrRecoveryRequired`，不猜测恢复结果；
- Data/Mapping active 文件使用 `O_RDONLY|O_NOFOLLOW`；invalid tail 返回 corruption，不 truncate；
- 不移动、删除、重命名或重写任何文件；
- 不提供自动 repair。

需要恢复时，operator 先用匹配版本正常 Open/Close Store，再重新离线 Verify。修复功能若以后增加，必须使用独立命令、备份与审计协议。

## 3. 验证范围

当前实现验证：

1. CURRENT、Manifest CRC/格式、Store UUID 与文件集合；
2. sealed Data Header/Frame CRC/SegmentSeal/Footer，active Data 完整物理 tail；
3. sealed/active Mapping Header、Node CRC、NodeSeq、Footer 与 Root 可达树；
4. 从 Manifest Root 开始，以只读 scanner 重放 ReplayStart 后 Commit/Relocation/Abort/Reserve，复用正式 recovery descriptor 校验；
5. 当前 Mapping 的每个 VAddr 必须指向相同 ID、非零 OriginBatchID 的 PutRecord；不同 ID 不能 alias 同一个 VAddr；
6. checkpoint Root 推导的 exact live bytes/records 必须等于同代 Manifest SegmentStats；
7. 正式 data/mapping 目录不能存在 Manifest 外文件，clean 状态下 trash 必须为空；
8. Mapping total/reachable/unreachable bytes 与 Data live/dead/system bytes 统计。

Scrub report 是诊断证据，不参与正常读写或 GC 删除授权。

## 4. 资源边界

第一版为了对 final Mapping 与物理 PutRecord 做精确 join，会在内存保存当前 live `VAddr -> ID` 和 checkpoint live 地址集合，因此额外内存为 `O(live IDs)`。Data Value 不保留，只顺序解码当前 Frame。超大 Store 应在独立维护节点运行并设置进程资源限制；后续可增加 external-sort/partitioned scrub，但不能降低验证语义。

当前 verifier 针对当前 Format Major；遇到不支持的版本返回 `ErrUnsupported`，不会尝试原地升级。

# storage-backup-pitr-go

本地单机可运行的**持久化键值存储**，以及在其之上的**全量备份 / 增量备份 / 任意时间点恢复（PITR）**服务。

- 写入先追加 WAL 并 `fsync`，进程崩溃后恢复到最后一个完整提交（被撕裂的尾部帧自动截断）。
- 全量备份以一次 **WAL Barrier（一致点）** 为准，备份进行中的并发写入不会进入该次备份。
- 增量备份只包含上次备份以来新封存的段，与全量备份构成可追溯链。
- 恢复可指定任意已提交的 CSN（提交序号）或提交时间，结果写入**独立目标目录**，可安全重试。
- 每个段、快照、清单都带校验和与帧级 CRC，损坏会被明确报错并定位到具体链 / 备份 / 段。
- 支持按**保留数量**或**保留时长**回收旧链，并安全回收引擎本地 WAL 段。

## 目录结构

```
internal/wal/      CRC 保护的追加式段日志（帧编码、滚动、严格校验、崩溃尾部截断）
internal/engine/   KV 引擎：内存 map + WAL 回放 + 带校验快照（snapshot）+ Barrier/回收
internal/backup/   全量/增量备份、链管理、PIT 恢复、保留回收
internal/server/   HTTP API
cmd/server/        可执行服务入口
```

引擎目录（`-data-dir`）布局：

```
<data-dir>/
  snapshot.gob        # 一致状态快照（CRC32 保护；回收旧段时生成）
  wal/
    seg-0000000000.log
    seg-0000000001.log # 最高序号为活动段，其余为不可变封存段
```

备份仓库（`-backup-dir`）布局：

```
<backup-dir>/
  chain-<id>/
    full-<id>/
      meta.json           # 清单：点 CSN/时间、段列表、各段 SHA-256、可选快照校验和
      snapshot.gob        # 全量点的引擎快照（若引擎此前回收过段则一定存在）
      segments/seg-*.log  # 该全量点所需的全部不可变段副本
    inc-<id>/
      meta.json
      segments/seg-*.log  # 相对上一备份新增的段
```

## 一致点（Consistency Point）的定义

1. 每次 `Put`/`Delete` 分配单调递增的 **CSN**，编码成一帧追加到活动段并 `fsync`；`fsync` 返回即“已提交”。
2. 备份开始时调用引擎 `Barrier`：对提交加锁，记录当前最新 CSN 与提交时间，然后 `Rotate` 封存活动段并 `fsync`、打开全新活动段。
3. 因此备份只复制 Barrier 之前已封存的不可变段。Barrier 之后并发提交的帧落在新活动段，物理上不可能出现在本次备份中。
4. 从备份恢复出的状态 = 按序重放 CSN `1..PointCSN` 的全部已提交帧，**恰好等于某个已提交时刻**：写入既不丢失也不重复（每帧有唯一 CSN，重放以快照为基、CSN 去重）。

## 恢复语义（PITR）

- 目标用 `target_csn`（精确提交序号）或 `target_time`（RFC3339Nano，取“提交时间 ≤ 目标时间”的最新提交）二选一指定。
- 恢复选择“全量 + 覆盖该目标点所需的若干增量”，先对每个源段做 SHA-256 与帧级 CRC 严格校验，再写入**独立目标目录**。
- 逐帧过滤，仅写出 `CSN <= 目标CSN` 的帧；目标点之后的写入不会出现。
- 恢复流程写入 `目标目录.restore-staging/` 暂存目录：
  - 重试时先删除旧暂存目录，重新构建；成功后用目录级原子 `rename` 替换目标目录。
  - 中途崩溃只可能留下暂存目录（下次清理），绝不会出现“半个目标目录”；重跑结果与一次成功完全一致。
- 暂存构建完成后会用引擎真正打开一次做严格校验（快照 CRC + 所有帧 CRC、CSN 必须恰好等于目标），通过后才切换。
- 恢复**只读**备份仓库，不触碰运行中的引擎、源备份或其它链。

## 损坏检测

- WAL 帧：`长度 | body | CRC32(body)`。封存段在打开 / 备份复制 / 恢复时做严格校验，任何错字节、截断、魔数错误都返回 `CorruptionError`（含段名与字节偏移）。
- 活动段允许“崩溃写到一半”的尾部（仅最后一帧不完整），打开时截断到最后一个完整帧——这属于崩溃恢复而非损坏。
- 备份：`meta.json` 记录每个段的大小与 SHA-256；恢复前逐段核对大小、SHA-256 并解码所有帧。错误信息形如
  `backup: corrupt: chain=<id> backup=<id> segment=<name> op=checksum|framing|stat: ...`，可直接定位损坏段。
- 快照：`snapshot.gob` 带魔数与 CRC32，损坏即报 `engine: corrupt snapshot`，绝不静默用坏数据启动。

“可恢复”与“损坏”的边界：只有**活动段**允许“最后一帧写了一半”的崩溃尾部（截断后正常启动）；
封存段的任何截断、长度错误、魔数错误、CRC/SHA-256 不符都判定为损坏并定位到具体段，不会静默恢复出错误状态。

可恢复点下限：若全量备份的段从 0 开始（未发生过本地回收），可恢复到该全量区间内的任意提交；
若引擎此前回收过旧段，全量备份会携带全量点快照，此时该链的可恢复下限为快照点 CSN，
更早的点物理上已被回收，恢复会以明确错误（“predates snapshot base”）拒绝而非给出错误状态。

## 保留策略

`Retention{ Count, Age }`（两者为“或”关系，命中任一即过期），每次全量备份后自动执行，也可通过 API 手动触发：

- `Count > 0`：仅保留 Tip 最新的 N 条链（按 Tip 提交时间排序）。
- `Age > 0`：丢弃 Tip 时间早于 `now - Age` 的链。
- 删除被淘汰链后，计算**仍保留**链中“全量备份引用的最早段索引”的最小值 `minFirst`，仅回收引擎本地 `index < minFirst` 的段：
  - 回收前先把当前一致状态写快照并 `fsync`、原子替换后再删段，崩溃任意时刻都可恢复。
  - 任何仍在保留范围内的链，其全量本身已包含所需段与（必要时）快照，可恢复性不受影响。
- 备份 / 增量 / 回收的元数据操作由服务级互斥锁串行化，保证并发下结果确定；不同链的复制互不污染。备份期间引擎的读写始终可用，仅在追加提交时短暂竞争引擎锁，复制只读不可变段，对正常写入影响受控。

## 运行

```bash
go build -o bin/kvsrv ./cmd/server
./bin/kvsrv \
  -addr :8080 \
  -data-dir ./data/engine \
  -backup-dir ./data/backups \
  -retention-count 3 \
  -retention-age 168h
```

可用环境变量：`ADDR`、`DATA_DIR`、`BACKUP_DIR`、`RETENTION_COUNT`、`RETENTION_AGE`（如 `168h`）。

### HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/healthz` | 健康检查 |
| `PUT` | `/v1/kv/{key}` | 写入，body `{"value":"..."}`，返回 `{csn, committed_at}` |
| `GET` | `/v1/kv/{key}` | 读取 |
| `DELETE` | `/v1/kv/{key}` | 删除 |
| `POST` | `/v1/backups/full` | 全量备份（新链），body 可含 `chain_id` |
| `POST` | `/v1/backups/incremental` | 增量备份，body `{"chain_id":"..."}` |
| `GET` | `/v1/backups` | 列出全部链（Tip 新→旧） |
| `POST` | `/v1/backups/restore` | PIT 恢复，见下 |
| `POST` | `/v1/backups/retention/apply` | 立即执行保留回收 |

恢复请求示例：

```json
POST /v1/backups/restore
{
  "chain_id": "chain-...",      // 省略则选 Tip 最新的链
  "target_csn": 12,             // 或用 "target_time":"2026-10-01T10:00:00Z"
  "dest_dir": "/var/restore/engine-12"
}
```

### curl 速览

```bash
curl -XPUT localhost:8080/v1/kv/a -d '{"value":"1"}'
curl localhost:8080/v1/kv/a
curl -XPOST localhost:8080/v1/backups/full -d '{}'
curl -XPOST localhost:8080/v1/backups/incremental -d '{"chain_id":"chain-..."}'
curl -XPOST localhost:8080/v1/backups/restore \
  -d '{"target_csn":1,"dest_dir":"/tmp/restored-engine"}'
```

恢复产物是一个标准引擎目录，可直接作为 `-data-dir` 重新启动服务。

## 测试

```bash
go test ./...
go test -race ./...
```

覆盖场景（见各包 `*_test.go`）：

- WAL：追加/重开放大、段滚动与复制、封存段 CRC 损坏定位、活动段崩溃尾部截断。
- 引擎：崩溃恢复、删除语义、Barrier、快照回收后仍可恢复、快照损坏报错。
- 备份：备份期间并发写入仍得到精确一致点、全量+增量链、按 CSN 与按时间的任意点恢复、段回收后新全量仍自洽、损坏段精确定位、恢复中断后重试结果一致、按数量/时长回收、多个备份任务并发且链互不污染。
- HTTP：读写 + 全量/增量/恢复端到端。

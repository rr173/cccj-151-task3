# 中继 API

基础地址默认 `http://localhost:8090`，请求/响应均为 `application/json; charset=utf-8`。
所有写操作在 SQLite 单库锁内串行；转发回调在锁外执行，慢网络不阻塞导入与上传。

## 状态与页面

| 方法 路径 | 说明 |
|---|---|
| `GET /` | 可操作控制台（执行包导入 / 终端下载 / 批量回执 / 队列重试 / 审计 / 模拟主系统） |
| `GET /api/health` | 健康检查，返回适配器类型与启动恢复条数 |

## 执行包导入（不可变、幂等）

### `POST /api/packages`
导入撤离编排系统导出的出站执行包。

```json
{
  "package_id": "EVAC-2026-10-02-01",
  "summary": "北岭火线 第 2 波撤离令",
  "issued_at": 1759400000,
  "batches": [
    {"batch_id": "B-N2-01", "vehicle": "V3", "origin": "G2",
     "destination": "S1", "households": ["H-101","H-102"],
     "expected_kinds": ["departed","arrived"]}
  ]
}
```

- 中继按规范化 JSON 计算 SHA-256 内容指纹。
- **完全相同的再次导入**：`200 {"idempotent_replay":true,...}`，返回首次导入结果。
- **同 `package_id` 但内容不同**：`409 invalid/package_id_reused`，拒绝覆盖（不可变）。
- 批次摘要指纹 = 规范化后的 `vehicle+origin+destination+households` 的 16 位前缀，
  随下载返回；回执必须携带，防止终端基于过期批次视图上传。

### `GET /api/packages`
执行包与全部批次列表：每批含游标 `next_seq`、终态屏障 `blocked_seq/blocked_reason`、
缺口 `gaps`、游标后缓冲项 `buffered_behind_cursor`、各状态计数 `states`、
派生状态 `derived_status`（waiting/awaiting_gap/retrying/forwarding/flowing/caught_up/blocked）。

### `GET /api/packages/{id}` / `.../detail`
执行包详情（含原始 payload）。

### `GET /api/packages/{id}/download`
**现场终端下载**：原始不可变执行包 `payload` + 每批次 `batch_summary` 指纹 + 内容指纹。

## 回执上传

回执类型 `kind`：`departed`（已离场）、`not_departed`（未能出发）、`arrived`（到达确认）。

### `POST /api/receipts`
单条上传：

```json
{
  "batch_id": "B-N2-01",
  "batch_summary": "2bf77d1904934ad6",
  "seq": 1,
  "receipt_no": "RC-2026-…-0001",
  "kind": "departed",
  "occurred_at": 1759400100,
  "details": {"odo_km": 0.3}
}
```

逐条结果（HTTP 200/409/422，体始终是结果对象，便于现场终端判读）：

| `status` | 含义 |
|---|---|
| `accepted` | 新回执已持久化（乱序时为缓冲） |
| `replay` | **完全相同的再次提交**：返回首次状态，不重复转发、不推进游标 |
| `conflict` (409) | 同 `(batch_id,seq)` 内容不同，或 `receipt_no` 被其它内容/序号复用：拒绝且**不推进游标** |
| `invalid` (422) | 未知批次、`batch_summary` 不一致、字段非法 |

### `POST /api/receipts/bulk`
体为 `{"receipts":[ … ]}`（也接受顶层数组）。响应：

```json
{"results":[…逐条同上…],
 "counts":{"accepted":2,"replay":1,"conflict":0,"invalid":0},
 "all_ok": true}
```

冲突/无效条目不影响同批其它合法条目；合法条目立即触发一次异步转发尝试。

## 转发与重试

### `POST /api/retry`
- `{}`：对所有未建屏障的批次执行一次安全重试/排空；
- `{"batch_id":"…"}`：只处理指定批次。
- 回调超时、连接失败、408/429/5xx 都是**可安全重试**：状态 `retry`、游标不动；
  后台泵（默认 2s）也会自动重试。适配器以 `Idempotency-Key: <receipt_no>`
  要求主系统幂等，因此重复到达不会在主系统二次生效。
- 主系统返回终态 `{"status":"terminal","reason":"withdrawn|vehicle_changed|destination_changed"}`
  时，该序号置 `rejected` 并在批次上建立 `blocked_seq` 屏障：**其后的任何序号
  永远不会被转发**（即使已缓冲/重试），直到主系统重新下发执行包。

### `GET /api/batches` / `GET /api/batches/{id}`
批次总览 / 批次详情：游标、屏障与中文原因、缺口、缓冲项、每条回执的
状态/尝试次数/失败原因/主系统受理引用/转发时间，以及批次原始执行单元 JSON。

## 审计

### `GET /api/audit?limit=&package_id=&batch_id=`
倒序审计事件：`package_imported`、`receipt_received`、`forwarded`、
`retry_scheduled`、`terminal_rejected`、`recovered`（启动恢复）。

## 演示：模拟主系统

仅在使用内置模拟主系统时有意义（`RELAY_ADAPTER=mock`，或 HTTP 适配器回调本进程的
演示端点 `/callbacks/relay-receipt`）：

| 方法 路径 | 说明 |
|---|---|
| `GET /api/mock/config` | 瞬时故障/终态规则、模拟延迟、**受理明细与每个回执编号的实际到达次数**（恰好一次证据） |
| `POST /api/mock/config` | `{"transient_times":{"B1":2}}` 前 2 次回调注入超时/5xx；`{"terminal":{"B1:1":"vehicle_changed"}}` 在指定序号建终态；`{"terminal_by_no":{"RC-…":"withdrawn"}}`；`{"latency":0.5}`；`{"reset":true}` 清空 |
| `POST /callbacks/relay-receipt` | 模拟主系统的 HTTP 回调端点（按回执编号幂等受理） |

## 主系统回调契约（适配器实现方对接用）

- 请求：`POST {RELAY_MAIN_URL}`，头 `Idempotency-Key: <receipt_no>`，体：

```json
{"package_id":"…","batch_id":"…","seq":3,"receipt_no":"…",
 "kind":"arrived","batch_summary":"…","occurred_at":…,"payload":{ …原始回执… }}
```

- 响应：
  - `200 {"status":"ack","ref":"<主系统受理引用>"}`
  - `200 {"status":"terminal","reason":"withdrawn|vehicle_changed|destination_changed","detail":"…"}`
  - `408/429/5xx` 或网络错误：中继安全重试（主系统必须按 `Idempotency-Key` 幂等）。

## 部署环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `RELAY_PORT` | 8090 | 监听端口 |
| `RELAY_DB` | `data/relay.db` | SQLite 路径（WAL，挂载到持久卷） |
| `RELAY_ADAPTER` | `mock` | `mock` / `http` / `module.path:factory` 自定义 |
| `RELAY_MAIN_URL` | 本进程演示端点 | http 适配器的回调地址 |
| `RELAY_HTTP_TIMEOUT` | 5 | 回调超时秒数 |
| `RELAY_PUMP_INTERVAL` | 2 | 后台转发泵轮询秒数 |

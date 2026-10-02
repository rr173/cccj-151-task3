# 离线车队回执中继（Offline Convoy Receipt Relay）

撤离编排系统与弱网现场之间的**独立**回执中继模块。编排系统通过**出站执行包**把批次下发给中继，
中继**不可变地持久化**这些批次包供现场终端下载；现场终端在车辆
**已离场 / 未能出发 / 到达确认**时上传带**批次摘要、回执编号、批次内递增序号**的回执；
中继在乱序、重复上传、网络中断下保证**按序转发与恰好一次效果**，并把缺口、缓冲项、
转发结果与失败原因完整展示。

- **零第三方依赖**：Python 3.9+ 标准库（`http.server` + `sqlite3` WAL），原生 HTML/JS 页面。
- **不可变批次包**：执行包内容指纹（规范化 JSON 的 SHA-256）随包保存；
  完全相同的再次导入返回首次结果，同 ID 不同内容直接 409 拒绝。
- **缺口缓冲 + 连续转发**：每批次一条序号流（游标 `next_seq` 从 1 起）。
  乱序回执全部持久化，但只有连续队首会被转发；缺口补齐后自动连续排空。
- **恰好一次**：回执按 `(batch_id, seq)` 与全局唯一 `receipt_no` 去重；
  转发先落盘 `forwarding` 再在锁外发起回调，结果落盘后才推进游标；
  回调携带 `Idempotency-Key`，超时/5xx 可安全重试，主系统不会二次生效。
- **冲突不推进**：同一序号或同一回执编号出现不同内容 → 409 拒绝，
  不插入、不推进游标，原结果不受影响。
- **终态屏障**：主系统返回**批次已撤回 / 车辆已变化 / 目的地已变化**时，
  该序号记为终态拒绝并建立屏障；其后序号即使已缓冲也永不越过。
- **重启恢复**：崩溃时停留在转发中的回执在启动时重置为待重试（按回执编号幂等重放）；
  已成功的回调绝不重复调用；待转发队列从 SQLite 完整恢复后继续处理。
- **可替换适配器**：内置进程内 `mock`（演示/测试）与 `http`（真实主系统），
  也可用 `RELAY_ADAPTER=module:factory` 注入自定义适配器。

## 快速开始

### 容器

```bash
cd relay
docker compose up -d --build
# http://localhost:8090
```

数据持久化在命名卷 `relay-data`。默认用 HTTP 适配器回调容器自身的演示端点
（模拟主系统）；对接真实编排系统时在 `docker-compose.yml` 设置
`RELAY_MAIN_URL=http://<主系统>/api/relay/callbacks`。

### 本地

```bash
cd relay
./run.sh                     # 或 python3 app/server.py
# http://localhost:8090
```

### 3 分钟演示

```bash
# 终端 1
RELAY_ADAPTER=mock ./run.sh
# 终端 2（导入示例包、给 B-N2-03 注入 2 次瞬时故障、乱序/顺序上传一批回执）
python3 seed_demo.py
```

1. **① 执行包与批次**：示例已导入（再次运行 seed 显示幂等重放）。
2. **③ 队列页**：`B-N2-01` 显示「等待补齐缺口」——#2/#3 已缓冲、#1 缺失，无任何回调发出；
   `B-N2-03` 显示「瞬时失败·待重试」，可见失败原因与尝试次数，后台泵稍后自动成功；
   **🧪 模拟主系统**页显示该回执到达 3 次但只受理 1 次。
3. 回到 **② 现场终端**：选择 `B-N2-01` →「补传序号 1」→ 批量上传；
   #1/#2/#3 在同一轮连续转发完成。
4. **🧪 模拟主系统**：给某批次「序号 1」配置终态（如车辆已变化）；
   在终端页上传 #1/#2/#3 —— #1 被终态拒绝，队列页出现红色屏障，#2/#3 永不越过。
5. 重复上传任意已传回执：返回「幂等重放」；把同一序号改个细节再传：409 冲突且游标不动。
6. `kill -9` 服务进程后重新 `./run.sh`：队列、屏障、审计全部恢复，
   已转发项不会再次回调。

## 页面

- **① 执行包与批次**：粘贴/上传 JSON 导入（内置示例）；按包展示批次状态、游标、屏障。
- **② 现场终端**：下载不可变执行包（含批次摘要指纹）；拼装批量回执清单，
  一键演示乱序（#2/#3）与补齐缺口（#1）；逐条显示接收/重放/冲突结果。
- **③ 队列·缺口·重试**：每批次的已转发/缓冲/待重试/终态计数与缺口；
  批次详情含序号轨道（成功/缓冲/重试/拒绝/缺口格）、每条回执的尝试次数、
  转发时间、主系统引用或失败原因；可单批或全部安全重试。
- **④ 审计**：导入、接收、转发成功、重试安排、终态拒绝、崩溃恢复全量审计流。
- **🧪 模拟主系统**：注入超时/5xx 次数与三类终态规则；展示主系统受理明细与
  每个回执编号的实际回调到达次数（恰好一次的直接证据）。

## API

详见 [API.md](API.md)。要点：

| 能力 | 接口 |
|---|---|
| 执行包导入（幂等/不可变） | `POST /api/packages` |
| 终端下载 | `GET /api/packages/{id}/download` |
| 单条 / 批量回执 | `POST /api/receipts`、`POST /api/receipts/bulk` |
| 队列与批次详情 | `GET /api/batches`、`GET /api/batches/{id}` |
| 失败安全重试 | `POST /api/retry`（可带 `batch_id`） |
| 审计 | `GET /api/audit` |
| 模拟主系统配置/受理证据 | `GET|POST /api/mock/config` |

## 一致性设计（为什么恰好一次）

1. **提交先于调用**：队首状态在事务内置 `forwarding`（`attempts+1`）并 fsync 后，
   才在锁外发起 HTTP 回调；回调结果回来再一次性落盘并推进 `next_seq`。
2. **崩溃点穷举**：
   - 崩溃在调用前：重启后 `forwarding → retry`，重发；主系统未见该编号，无副作用。
   - 崩溃在返回后、落盘前：重启后重发；主系统按 `Idempotency-Key` 返回首次受理，不重复生效。
   - 崩溃在落盘后：状态已是 `forwarded`，重启泵直接跳过，绝不二次调用。
3. **顺序屏障**：瞬时失败时游标停在失败序号，已到达的后续序号只能缓冲；
   终态拒绝把屏障永久固化，任何重试与后续回执都不能越过。
4. **串行化**：单 SQLite 连接 + RLock 处理所有读改写；同一批次用非阻塞锁串行，
   不同批次的慢回调互不阻塞（各自在独立线程）。

## 自定义回调适配器

```python
# mycompany/ad_relay.py
class MyAdapter:
    name = "myorg"
    def __init__(self, config): ...
    def callback(self, env: dict):
        # env: package_id/batch_id/seq/receipt_no/kind/batch_summary/occurred_at/payload
        from app.adapters import CallbackResult   # 或直接返回三元组风格的结果对象
        return CallbackResult("ack", ref="...")
        # return CallbackResult("transient", "timeout", "可重试原因")
        # return CallbackResult("terminal", "withdrawn", "终态原因")
```

```bash
PYTHONPATH=/srv RELAY_ADAPTER=mycompany.ad_relay:MyAdapter python3 app/server.py
```

`CallbackResult.outcome` 三态语义即主系统判定：`ack` / `transient`（安全重试）/
`terminal`（`withdrawn` / `vehicle_changed` / `destination_changed`）。
**适配器必须保证按 `receipt_no` 幂等**（中继可能在结果丢失时重放同一编号）。

## 测试

```bash
python3 tests/test_relay.py          # 核心语义 17 项：不可变导入、缺口缓冲、
                                     # 幂等重放、冲突不推进、三类终态屏障、重启恢复
python3 tests/test_http_adapter.py   # 适配器协议 6 项：ack/terminal/408/429/5xx/拒连分类
python3 tests/e2e_test.py            # 真实端口端到端 8 项：含 SIGKILL 后进程重启恢复
```

## 目录

```
relay/
  app/
    db.py        SQLite(WAL) 表结构、终态常量、崩溃恢复 recover_inflight
    models.py    规范化 JSON / 内容指纹 / 批次摘要
    adapters.py  CallbackResult、MockMainSystem、MockAdapter、HttpAdapter、自定义装载
    relay.py     导入/接收/冲突判定/转发泵/终态屏障/视图
    server.py    HTTP 路由 + 演示回调端点
  web/           index.html styles.css app.js（5 个可操作页面）
  tests/         核心语义 / 适配器协议 / 端到端重启
  examples/sample_package.json
  Dockerfile docker-compose.yml run.sh seed_demo.py API.md
```

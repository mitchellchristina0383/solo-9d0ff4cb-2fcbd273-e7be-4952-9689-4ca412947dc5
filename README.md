# Dependency Alert API

值班工程师面对一串服务故障告警时，需要先处理真正的**上游根因故障**，同时保留尚未恢复的
下游告警但予以抑制。本服务用 **FastAPI + SQLite（WAL）** 实现：

- 整版提交服务依赖图（`A` 依赖 `B`，则 `B` 是 `A` 的上游）；
- 带**每服务递增正整数序号**上报故障 / 恢复，支持幂等重试、冲突拒绝、迟到不回退；
- 查询时动态计算**需处理故障**与**被抑制故障**及其上游根因；
- 替换依赖图时按服务名保留状态，恢复后下游自动重新归类，无需重报；
- 所有写操作走序列化事务，并发上报 / 换版 / 查询不会混合不同已提交状态；
- **HTTP 告警投递**：告警进入 / 退出「待处理」时向接警端发送 `open` / `close`
  通知，带事务性发件箱、重试、去重与按服务保序（见下文）。

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
# API:        http://localhost:8000
# API 文档:   http://localhost:8000/docs   （Swagger UI，可直接操作）
# 健康:       http://localhost:8000/health
# 接警端:     http://localhost:9000/docs   （接收记录 + API 待发通知）
# 接警端健康: http://localhost:9000/health
```

SQLite 数据文件保存在命名卷 `alert-data`（容器内 `/data/alerts.db`）。
`notify-receiver` 是本地接警端容器，API 启动时会按 `NOTIFY_RECEIVER_URL`
（默认 `http://notify-receiver:9000/alerts`）投递通知，无需手工配置。

### 本地直接运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
ALERT_DB_PATH=./data/alerts.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/graph` | 整版替换依赖图。校验失败（环路 / 未知引用 / 重名 / 重边 / 自依赖）返回 `422` 并**保留旧版** |
| `GET` | `/graph` | 读取当前已提交图（含内部修订号 `revision`） |
| `POST` | `/reports` | 上报故障 / 恢复 |
| `GET` | `/alerts` | 查询告警分类（需处理 / 被抑制 / 根因 / 路径） |
| `GET` | `/notifications` | 查看通知发件箱：`pending` / `sending` / `delivered` 及每行重试信息 |
| `POST` | `/admin/reset` | 清空全部数据（测试 / 运维辅助） |
| `GET` | `/health` | 健康检查 |

### 提交依赖图

```json
POST /graph
{
  "version": "v1",
  "services": [
    {"name": "A", "dependencies": ["B", "C"]},
    {"name": "B", "dependencies": ["D"]},
    {"name": "C", "dependencies": ["D"]},
    {"name": "D", "dependencies": []}
  ]
}
```

`dependencies` 中列出的是**上游**服务；所有被引用服务必须在同一次提交中出现，
整图必须是 DAG（Kahn 算法检环）。每次成功提交 `revision` 递增。

### 上报故障 / 恢复

```json
POST /reports
{"service": "B", "seq": 7, "status": "fault"}
```

序号 `seq` 为**每个服务各自维护的严格递增正整数**，规则：

| 情况 | 结果 |
| --- | --- |
| `seq > last_seq` | 应用新状态 |
| `seq == last_seq` 且内容相同 | 幂等重试，`applied=false, duplicate=true`，状态不变 |
| `seq == last_seq` 但内容不同（fault/recover 互斥） | `409 conflict`，状态不变 |
| `seq < last_seq`（迟到上报） | `409 conflict`，**绝不回退状态** |
| 服务不在当前图中 | `404 unknown_service` |
| `seq <= 0` | `422`（请求体校验） |

序号允许有空洞（如 1 之后直接 3）。每次上报（含被拒绝的）都写入 `events` 审计表。

### 查询告警

```json
GET /alerts
{
  "graph_revision": 1,
  "graph_version": "v1",
  "services": ["A", "B", "C", "D"],
  "actionable_faults": [{"service": "D", "last_seq": 1}],
  "suppressed_faults": [
    {
      "service": "B",
      "last_seq": 1,
      "root_causes": ["D"],
      "paths": [["B", "D"]]
    }
  ],
  "down_services": ["B", "D"],
  "unreported_services": ["C"],
  "healthy_services": []
}
```

## 抑制规则（核心语义）

1. 只有当前仍处于故障（down）状态的服务才会出现在 `actionable_faults` 或
   `suppressed_faults` 中，两类互斥且并集恰为全部 down 服务。
2. **根因**：故障且没有任何故障上游的服务。根因永远是需处理故障。
3. 一个非根因故障被抑制，当且仅当存在一条到达某根因的依赖路径，且**路径上每个节点都
   处于故障中**。
4. 健康节点或**从未上报**的节点会截断路径：其下游即使故障也不能经由该路径被抑制；
   若所有路径都被截断，该故障仍归入需处理。
5. 分类是查询时根据当前图 + 当前状态**动态计算**的。服务恢复、依赖图换版后立即重新
   归类，不需要下游重新上报。

## 换版与状态保留

- 服务状态（up/down、最后序号）独立于图存储，按服务名为主键。换版后同名服务的状态与
  序号历史原样保留；被移除服务的状态行不删除但不会出现在查询中，将来重新加入时恢复可见。
- 新图中首次出现的服务归入 `unreported_services`。

## 并发模型

- SQLite 开启 WAL；每个写事务以 `BEGIN IMMEDIATE` 开始，写者全局串行化，遇
  `SQLITE_BUSY` 短时重试。
- 读事务持有单一已提交快照，因此换版与查询并发时，每次 `/alerts` 看到的节点集、边与
  修订号必然属于同一个已提交版本，不会出现混合状态。
- 每个请求使用独立的线程级连接，请求结束即关闭。

## 告警通知投递（给接警端）

服务一旦进入「需处理告警」（`actionable_faults`）集合就视为**待处理**。投递的目标是
「同一服务待处理状态发生**边沿变化**时通知接警端」：

| 变化 | 通知 |
| --- | --- |
| 有效上报使服务**进入**待处理 | `action=open`（`reason=report`） |
| 换图后服务**新进入**待处理 | `action=open`（`reason=graph_version`） |
| 恢复导致**退出**待处理 | `action=close`（`reason=recovered`） |
| 仍故障但换图后变为被抑制 | `action=close`（`reason=suppressed`） |
| 换图后服务被移出图 | `action=close`（`reason=removed_from_graph`） |
| 持续待处理（重复上报 / 序号递增但状态不变 / 仍被抑制） | **不重复发送** |
| 关闭后再次进入 | 新的一轮 `open`，`round` 递增 |

「待处理」只包含**根因**（需处理故障），被抑制故障不会打开通知：它们的处置依附于根因，
根因恢复后下游会立即重新归类并按上表补发。判断完全复用 `/alerts` 的分类逻辑，所以
恢复、被抑制、换图、移出图都会通过与上报/换版相同的事务被观察到。

### 事务性发件箱

通知行与触发它的那次写操作在**同一个 SQLite 事务**中写入
（`alert_notifications` + `notification_state`）：

- 上报 / 换图成功 → 状态变更与通知行同时可见；
- 通知写入失败 → 整个事务回滚，**不会出现「告警变了但没有通知」**；上报请求返回 `500`，
  客户端可安全重试（序号语义不变）；
- 投递失败**永远不会回滚告警**：网络发送发生在事务之外，失败只把发件箱行放回待发。

### 投递语义

- 后台投递线程池（`NOTIFY_WORKERS`，默认 2）持续扫描发件箱；**不在请求线程里发网络请求**。
- **重试**：接警端非 2xx / 超时 / 连接失败都会重试，指数退避
  （`NOTIFY_RETRY_BASE_SECONDS` 起，封顶 `NOTIFY_RETRY_CAP_SECONDS`）。每次重试**沿用同一
  通知 ID** 与同一 payload。
- **按服务保序**：每服务维护单调 `service_seq`；发件箱只有在该服务更早的行都已
  `delivered` 时才允许投递后一行。一个服务的失败只阻塞它自己。
- **不抢发**：投递前在 `BEGIN IMMEDIATE` 事务中把行置为 `sending` 并加租约
  （`NOTIFY_LEASE_SECONDS`）。其他投递线程（包括另一个 API 进程）无法再次领取；
  进程崩溃后租约会过期，行恢复可领取（至少一次投递）。
- **重启续投**：未送达的行持久化在 SQLite 中，重启后后台线程继续投递。
- **接警端去重**：接收端按主键 `id` 去重；由于极端情况下可能「已送达但确认前崩溃」
  导致重复发送，接收端会把重复按 `200` 应答并只记一次。
- 超过本服务顺序阻塞不会无限期阻塞其他服务：投递顺序按 `service_seq` 的行号选取，
  一个卡住的服务不会让其他服务饿死。

通知 payload（`alert_notifications.payload`，也是投递给接警端的 JSON）：

```json
{
  "id": "c0ffee...",
  "service": "D",
  "action": "open",
  "service_seq": 1,
  "round": 1,
  "reason": "report",
  "graph_revision": 1,
  "graph_version": "v1",
  "created_at": 1794931963.58
}
```

`GET /notifications` 返回发件箱汇总与每行状态，`/docs`（接警端页面）同时展示已接收记录
和 API 待发通知（通过 `/upstream/notifications` 代理）。

### 接警端（`receiver/`）

标准库 `http.server` 实现，仅用于本地联调：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/alerts` | 接收一条通知；按 `id` 去重 |
| `GET` | `/alerts` | 已接收通知 + `duplicate_deliveries` 计数 |
| `GET` | `/docs` | 可视化页面：接警记录 + API 未送达通知 |
| `GET` | `/upstream/notifications` | 代理 API 的 `/notifications` |
| `POST` | `/admin/fail` | 测试钩子：`{"count": n}` 让接下来 n 次投递失败 |
| `POST` | `/admin/reset` | 清空接收端状态 |

## 自测

`tests/selftest.py` 仅依赖标准库，对运行中的服务做端到端校验：

```bash
python tests/selftest.py                      # 默认 http://127.0.0.1:8000
BASE_URL=http://localhost:8000 python tests/selftest.py
# 容器内：docker compose exec alert-api python tests/selftest.py
```
覆盖：图校验与原子替换、序号幂等 / 冲突 / 迟到语义、全故障路径抑制与健康 / 未上报截断、
恢复即时重分类、换版状态保留，以及同序号并发重试、同序号异内容竞争、多序号乱序竞争、
并发换版、换版与查询交错等关键并发场景（共 55 项断言）。

`tests/notification_integration.py` 覆盖通知投递：开 / 关 / 新一轮边沿语义、抑制与换图
触发的打开与关闭、接警端失败重试与按 id 去重、按服务保序、两个 API 进程共库不抢发、
API 重启后续投、通知写入失败回滚上报、旧库启动补发等（共 43 项断言）。它会自行拉起接收端与 API
进程（临时数据库），同样只依赖标准库：

```bash
PYTHONPATH=.pylibs:. python tests/notification_integration.py
```

## 目录结构

```
app/
  main.py            FastAPI 路由与异常映射
  database.py        SQLite 连接、schema、事务管理
  graph_service.py   图校验、整版替换、告警分类
  report_service.py  上报序号语义
  notifications.py   事务性发件箱：边沿检测、入队、领取/完成/重试
  delivery.py        后台投递线程池（HTTP、重试、租约）
  schemas.py         Pydantic 模型
  errors.py          领域错误
receiver/
  server.py          本地接警端（去重、/docs 页面）
tests/selftest.py    端到端自测（标准库）
tests/notification_integration.py  通知投递集成自测（标准库）
Dockerfile, receiver/Dockerfile, docker-compose.yml, requirements.txt
```

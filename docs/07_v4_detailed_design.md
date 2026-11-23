# EvalReel v4.0 详细设计文档

## 1. v4.0 目标、轻量优先原则与双模式对照

### 1.1 设计原则：全部新能力均内置进单文件轻量版

v1.0 到 v3.0 已经实现了大模型侧透明代理、第 N 步分叉横评、隔离子进程 Agent 评测、双模式调度和调用链瀑布图。v4.0 在此基础上补齐两部分能力：

1. **工具协议层盲区（MCP 透明代理、只读工具 Mock 与工具侧混沌注入）**：此前拦截和注入故障的都是大模型 API 通道，Agent 通过 MCP（Model Context Protocol，大模型连接外部工具的通用协议）调外部工具时看不到过程、也无法离线替身测试。v4.0 将“代理录制 + Mock + 混沌注入”完整延伸到 MCP 工具调用层。
2. **生产级运维与对比闭环**：补充失败子任务与死信队列的一键重放、跨多个历史版本的回归退化对比（`compare.html`）、基于 `go:embed` 内嵌进二进制文件的零构建轻量状态页，以及 GitHub Actions 自动竞态测试流水线。

**关键边界约定**：上面每一项 v4.0 新能力都在 **单机默认模式（`--mode=local`）** 中完整可用，不强迫本地使用者安装 MySQL、Redis 或 Node.js；切换到 `--mode=server` 集群模式时，自动对接 MySQL 与 Asynq 扩展死信队列操作。

### 1.2 v4.0 双模式能力对照表

| v4.0 新增能力 | 轻量单机模式（`--mode=local`，默认） | 分布式服务端模式（`--mode=server`） |
| --- | --- | --- |
| **MCP 透明代理 / Mock / 故障注入** | `evalreel mcp-proxy` 单独一条命令启动（Stdio 或 HTTP），磁带存为 `.mcp.jsonl.gz`，`trace_spans` 可直接写入 SQLite | 同一份 MCP 代理逻辑，可挂载 `MySQLStore` 将工具 Span 汇总进集群战报调用树 |
| **失败子任务一键重放** | `evalreel retry -run <run_id>` 与 `POST /api/runs/:id/retry-failed`，重放 SQLite 中 `status=failed` 子任务，跳过已成功任务 | `POST /api/runs/:id/retry-failed` 重置 MySQL 失败任务并重新投递至 Asynq；额外开放 `GET/POST /api/queues/archived` 管理 Asynq 死信队列 |
| **多版本回归趋势对比** | `evalreel compare -runs r1,r2,r3` 与 `GET /api/compare`，读取 SQLite 生成离线 `compare.html` | 共用同一套 `report.CompareRuns` 逻辑读取 MySQL，返回趋势统计与 `compare.html` |
| **极简状态页面** | `evalreel serve` 访问根路径 `GET /` 即打开内嵌单文件页面，支持实时进度、战报预览、重跑失败与勾选回归对比 | 完全相同的内嵌静态页，零前端打包依赖，可直接查看分布式队列堆积状态 |

### 1.3 新增模块目录规划

```text
EvalReel/
├── .github/workflows/
│   └── ci.yml                       # Step 5: GitHub Actions 自动编译与 -race 测试
├── cmd/evalreel/
│   ├── main.go                      # 注册 mcp-proxy、retry、compare 子命令
│   ├── mcp_proxy.go                 # Step 1-2: evalreel mcp-proxy CLI 入口
│   └── ops.go                       # Step 3-4: evalreel retry 与 compare CLI 入口
├── internal/
│   ├── mcpproxy/                    # Step 1-2: MCP 透明代理核心包
│   │   ├── jsonrpc.go               # JSON-RPC 2.0 报文、tools/call 请求与响应结构解析
│   │   ├── tape.go                  # .mcp.jsonl.gz 录制、读取与规范化参数哈希 Mock 匹配
│   │   ├── chaos.go                 # 工具侧 5 类故障注入规则引擎
│   │   ├── stdio.go                 # Stdio 子进程包装代理（逐行双向透传 + 旁路拦截）
│   │   └── http.go                  # Streamable HTTP MCP 反向代理（兼容 JSON 与 SSE）
│   ├── api/
│   │   └── ui/
│   │       └── index.html           # Step 5: go:embed 内嵌零构建状态与对比页（纯原生 HTML/JS）
│   └── report/
│       └── compare.go               # Step 4: 多版本回归退化计算与 compare.html 模板
└── scripts/
    └── mock_mcp_server.py           # Step 1-2 测试靶子：约 60 行标准库 Python Stdio/HTTP MCP 演示服务
```

---

## 2. MCP 透明代理、只读工具 Mock 与混沌注入（`internal/mcpproxy`）

### 2.1 为什么同时支持 Stdio 与 Streamable HTTP 两种传输

MCP 协议在实际使用中主要跑在两条传输通道上：
- **Stdio 模式**：本地 AI 编辑器或 Python Agent 直接通过标准输入输出管道（`stdin` / `stdout`）拉起本地 MCP 工具进程，每次调用发一行 JSON-RPC 2.0 报文。EvalReel 以“包装子进程”的方式插入中间：上游 Agent 改为启动 `evalreel mcp-proxy -mode=stdio -- python tool_server.py`，即可无感完成全部工具调用拦截、录磁带、Mock 或注入故障。
- **Streamable HTTP 模式**：远程或团队共享的工具服务通过 HTTP POST（响应可能是单个 JSON，也可能是 `text/event-stream` 流）对外暴露 `/mcp`。EvalReel 启动本地 HTTP 代理端口转发请求并旁路解析 JSON-RPC 载荷。

两种模式共用同一套 JSON-RPC 处理器，仅外层读写传输通道各自适配。

### 2.2 JSON-RPC 2.0 拦截与请求响应匹配

在 MCP 中，工具调用围绕标准 JSON-RPC 2.0 报文展开：
1. **建立请求追踪表**：代理读到来自客户端的请求 `{"jsonrpc":"2.0", "id": 7, "method": "tools/call", "params": {"name": "get_poi", "arguments": {...}}}` 时，记录下请求 `id`、`method`、工具名 `tool_name`、规范化参数、开始时间以及随请求头或环境变量传入的 `traceparent`。
2. **关联异步返回**：当服务端返回同一 `id` 的响应或 SSE 事件时，从追踪表取出对应的请求信息计算耗时并落盘为一条 `MCPRecord`（存进 `.mcp.jsonl.gz`）。
3. **联动多 Agent 调用树**：当配置了 `-db` 或 `-mysql-dsn` 时，代理同时把这条 `tools/call` 写成 `store.TraceSpan`（字段 `operation = "mcp_tool"`、`agent_name = "mcp:" + tool_name`），复用 v3.0 的 `trace_id` 与 `parent_span_id` 规范，直接在 `report.html` 瀑布图里看见 Agent 在第几毫秒调了哪个 MCP 工具、是否被注入故障。

### 2.3 只读工具离线 Mock 匹配算法（磁带回放）

为了让回放测试稳定且可预期，Mock 模式聚焦在**确定性只读查询工具**（如查天气、查文档、查地理坐标），不承诺对带写库副作用的操作做模糊猜测：

1. **规范化缓存键**：在录制 `.mcp.jsonl.gz` 与匹配 Mock 请求时，把 `arguments` JSON 对象按键名字母序递归重排并序列化成紧凑 JSON（解决 Go/Python 字典字段顺序不同导致同一参数匹配失败的问题），拼出 `key = sha256(tool_name + "|" + canonical_json(arguments))`。
2. **多次调用序号兜底**：如果同一场任务用完全相同参数连续调用两次同一工具且返回值不同，按 `(key, call_seq)` 顺序索引；仅调用一次或参数不同时直接按 `key` 命中。
3. **两种未命中策略（`-mock-fallback`）**：
   - `strict`（默认，完全断网测试用）：未命中录制磁带时直接返回 JSON-RPC `-32004` 错误提示缺磁带记录，绝不偷调真实下游；
   - `passthrough`（混合路由用）：未命中磁带时放行转发给真实 MCP 服务并可同步补录。

### 2.4 工具侧 5 类混沌故障注入

在代理配置规则中，支持按工具名（支持精确名或 `*` 通配）与触发间隔 `every_n` / 概率 `rate` 注入工具层异常，专门测试 Agent 主循环面对工具出错时的容错能力：

| 故障动作（`action`） | 代理注入行为 | 测试目标 |
| --- | --- | --- |
| `delay` | 在转发前等待固定毫秒数 `delay_ms` | 验证 Agent 工具超时设置与界面阻塞处理 |
| `timeout` | 等待超过阈值或主动阻断等待触发客户端超时 | 验证 Agent 超时重试与降级话术 |
| `jsonrpc_error` | 不发给真实工具服务，直接返回标准 JSON-RPC `error` 对象（自定义错误码与提示） | 验证 Agent 对 MCP 协议层异常的捕获 |
| `tool_is_error` | 返回 JSON-RPC 成功框架，但 `result.isError = true` 并带上错误信息文本 | 验证大模型在模型层看到工具失败提示后能否自我纠偏 |
| `malformed_json` | 在响应体中截断或掺入非合法 JSON 字符 | 验证 Agent MCP 客户端的反序列化防崩溃能力 |

---

## 3. 失败子任务与死信队列一键重放（Local + Server 双模式）

### 3.1 存储层状态重置（`Store.ResetFailedTasks`）

v2.0 的断点续跑（`-resume`）只处理 `pending` 与中断前卡在 `running` 的子任务，已经落入 `failed` 的子任务不会再次执行。而在真实跑批时，常常因为外部 API 短暂故障或个别超时导致整场 100 题里有 5 题变成 `failed`。v4.0 在 `Store` 接口新增统一方法：

```go
ResetFailedTasks(ctx context.Context, runID string) (int64, error)
```

在一个数据库事务内完成三步原子更新：
1. 把 `WHERE run_id = ? AND status = 'failed'` 的子任务状态重设为 `pending`、`error_msg` 清空（保持其他 `success` 子任务原封不动，避免重复花钱调用已完成题目）；
2. 根据实际重置条数 `n`（当 `n > 0` 时），把对应 `eval_runs` 记录的 `done_tasks` 减去 `n`、`failed_tasks` 减去 `n`（下限保护不小于 0）；
3. 把 `eval_runs.status` 重设回 `running`、`finished_at` 置为 `NULL`。

### 3.2 双模式下的触发方式

- **Local Mode（命令行 + 本地 API）**：
  - CLI：`evalreel retry -run <run_id> -db ./evalreel.db` 先调用 `ResetFailedTasks`，若重置条数 `> 0`，直接复用现有单机 `Scheduler` 仅跑这几条重置后的任务并刷新生成最新 `report.html`。
  - HTTP：`POST /api/runs/:id/retry-failed` 触发同样的重置与后台异步重跑，立即返回 `202 Accepted` 与重置任务数 `{"retried_tasks": n}`。
- **Server Mode（分布式集群 + Asynq 死信管理）**：
  - `POST /api/runs/:id/retry-failed` 执行 `MySQLStore.ResetFailedTasks` 后，通过 `ClaimPendingTasks` 捞出这批刚恢复为 `pending` 的任务重新投递进 Asynq（注意：重新入队前若原 `TaskID` 还停留在 Asynq Archived 或 Completed 保留记录里，先调用 `asynq.Inspector.DeleteTask("default", tk.ID)` 清理旧排队记录再 `EnqueueContext`，防止被旧任务号去重拦掉）。
  - 额外提供 `GET /api/queues/archived` 查看当前死信队列明细，以及 `POST /api/queues/archived/retry`（支持按 `task_id` 列表或 `all: true` 调用 `inspector.RunTask` / `RunAllArchivedTasks` 一键重新入队消费）。

---

## 4. 多版本回归趋势对比（`internal/report/compare.go`）

在研发 Agent 时，经常出现连续改了三版提示词或升级了两个模型版本，需要看清“每次改动到底提升了多少，又让哪些原本能做对的题目退化了”。

### 4.1 聚合数据结构与计算规则

`CompareRuns(runs []RunWithTasks)` 接口同时服务 CLI、Local API 和 Server API，输入按时间排序的 2 到 N 场 `EvalRun` + `EvalTask` + `EvalScore`（可选按 `target_name` 过滤只比同一个模型或策略）：

1. **版本总览走势**：输出每个版本的 `RunID`、`Name`、`CreatedAt`、`Accuracy`、`PassPowerK`、`AvgTTFTMs`、`P50Ms`、`P95Ms`、`TotalCostUSD` 以及相对上一版本的增量（`AccuracyDelta`、`P95DeltaMs`、`CostDeltaUSD`）。
2. **逐题退化/提升分类（针对基准版本 vs 最新版本，或跨全部版本矩阵）**：
   - **Regression（回归退化）**：前一版本通过，最新版本失败；
   - **Improved（修复提升）**：前一版本失败，最新版本通过；
   - **StableFail（持续失败）**：多个版本均未通过；
   - **StablePass（持续稳定）**：所有对比版本均通过。

### 4.2 离线 `compare.html` 与入口

- **CLI**：`evalreel compare -runs run_a,run_b,run_c -out ./reports/compare.html`（支持通过 `-db` 读本地 SQLite，或通过 `-mysql-dsn` 读 MySQL）。
- **HTTP**：`GET /api/compare?runs=run_a,run_b`（追加 `&format=json` 返回结构化 JSON 供前端或脚本消费，默认直接返回离线 `compare.html`）。
- **页面呈现**：页面顶层用纯内联 SVG 折线图画出多版本准确率、`pass^k` 与 P95 延迟变化趋势；下方逐题对比表默认把 **回归退化（Regression）** 排在最顶部高亮标红，帮助开发者几秒内定位被改坏的样本。

---

## 5. 内嵌极简状态页（`go:embed`）与 GitHub Actions CI

### 5.1 零前端构建链的 `go:embed` 状态页

为了延续 v2.0 审计报告确立的“不堆砌 Vue/React 前端工程”原则，同时让单机版和服务端版都有直观的可视化控制台，在 `internal/api/ui/index.html` 编写一个约 150 行的原生 HTML + CSS + Fetch/EventSource 单文件网页，通过 Go 1.16+ 标准特性 `//go:embed ui/index.html` 直接打进 `evalreel` 编译产物，挂载于 `evalreel serve` 的 `GET /`：

1. **顶部状态与触发栏**：展示健康状态与队列堆积概览（`/api/queues/stats`），支持粘贴 JSON 配置提交新任务并自动建立 SSE 连接显示实时完成进度条（`EventSource('/api/runs/<id>/events')`）。
2. **历史评测列表与一键操作**：轮询 `/api/runs`，每行直接提供三个按钮：打开离线战报（`/api/runs/:id/report`）、重跑失败任务（`POST /api/runs/:id/retry-failed`）、勾选框。
3. **多版本回归入口**：勾选 2 场及以上评测记录后点击“生成回归对比”，直接打开 `/api/compare?runs=...` 渲染趋势与退化页面。

整个状态页不需要安装 Node.js 或任何前端构建工具，单二进制文件随手启动浏览器访问 `http://127.0.0.1:8081` 即可使用。

### 5.2 GitHub Actions 自动 CI（`.github/workflows/ci.yml`）

在每次向 `main` 分支推送代码或提交 Pull Request 时，自动在 GitHub Ubuntu 容器内执行：
1. 安装 Go（对齐 `go.mod` 版本）；
2. 拉取依赖并运行带竞态检测的单元测试：`go test -race -count=1 ./...`；
3. 验证全项目二进制编译与多阶段校验：`go build -trimpath ./cmd/evalreel` 与 `go vet ./...`。

---

## 6. v4.0 五步分阶段实施与验收路线图

| 步骤 | 核心模块与改动范围 | 验收标准与单测要求 |
| --- | --- | --- |
| **Step 1** | `internal/mcpproxy/` 核心框架：JSON-RPC 2.0 解析、`.mcp.jsonl.gz` 磁带录制/规范化参数哈希、Stdio 与 Streamable HTTP 代理、`scripts/mock_mcp_server.py` | 不启动真实大模型即可跑通 Stdio 与 HTTP 两个通道的代理透传、录制 `.mcp.jsonl.gz` 以及写入 `trace_spans`；带 `-race` 与 `goleak` 单测通过。 |
| **Step 2** | `internal/mcpproxy/` Mock 回放与混沌引擎 + `evalreel mcp-proxy` CLI 子命令 | 验证只读工具离线断网 Mock（参数字段顺序打乱仍精确命中）、`passthrough` 降级与 5 类工具故障注入（`delay`/`timeout`/`jsonrpc_error`/`tool_is_error`/`malformed_json`）单测全部通过。 |
| **Step 3** | 失败子任务与死信队列一键重放：`Store.ResetFailedTasks`（SQLite + MySQL）、Asynq 死信查看与重投、`/retry-failed` 与 `/api/queues/archived/*` 接口、`evalreel retry` 命令 | 单机 SQLite 模式和分布式 Docker MySQL+Asynq 模式均验证“仅重跑失败任务，已成功任务计数不漂移且不重复执行”；`go test -race ./...` 全通过。 |
| **Step 4** | 多版本回归趋势对比：`internal/report/compare.go`、`evalreel compare` CLI 与 `GET /api/compare` 接口 | 构造 3 场评测样本验证准确识别退化题（Regression）与修复题（Improved），输出零外链离线 `compare.html`（无 `ZgotmplZ` 与 CDN 依赖）。 |
| **Step 5** | `internal/api/ui/index.html`（`go:embed` 挂载 `GET /`）、`.github/workflows/ci.yml`、全量回归测试与文档更新 | 验证单二进制 `evalreel serve` 根路径正常加载与操作、Docker Compose 集群全链路回归通过、更新 `README.md` 并推送验证 GitHub Actions CI 全绿。 |

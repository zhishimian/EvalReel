# EvalReel v3.0 详细设计文档

## 1. v3.0 目标、双模式架构与目录规划

### 1.1 核心目标

v3.0 在完整保留 v1.0 与 v2.0 单文件零依赖本地运行能力的前提下，重点补齐三块工程能力：

1. **分布式服务端模式（Server Mode）**：支持拉起「1 个 Gin 控制面节点 + 2 个及以上独立 Worker（后台工作进程）节点 + MySQL 8 + Redis 7」组合，覆盖 MySQL 状态机、基于 Asynq（Go 常用的 Redis 后台任务队列库）的任务持久化与死信重试、基于 Redis Lua 脚本（在 Redis 内部原子执行的轻量脚本）的全局限流、基于 Redis Pub/Sub（发布订阅消息通道）的跨进程实时进度推送，以及接口幂等防重。
2. **可观测性与多 Agent 白盒调用树**：统一接入结构化日志 `log/slog`、Prometheus（开源监控指标采集系统）指标暴露与 OpenTelemetry（开放遥测标准）链路导出；同时补齐 v2.0 留到 v3.0 的多 Agent 嵌套调用树追踪，利用 W3C 标准请求头 `traceparent` 把父子 Agent 的调用层级串成树形结构，并在离线 `report.html` 中绘制调用瀑布图。
3. **索引验证与分布式高并发压测**：通过向 MySQL 批量写入 20 万至 50 万条模拟子任务数据并用 `EXPLAIN` 验证查询索引命中情况；在多容器环境下，对带有真实首字延迟、持续流式分块与 5% 随机 429 报错的 `mockllm` 开展长时并发压测并收集 `pprof`（Go 内置性能剖析工具）数据。

### 1.2 双模式架构对比（Local Mode 与 Server Mode）

为了守住项目初期“个人开发零依赖上手”的目标，v3.0 通过 `--mode` 参数将单机模式（默认）与分布式服务端模式隔离开，共用一套业务执行器（`runner`）、打分器（`scorer`）和报告引擎（`report`）：

| 维度 | Local Mode（单机默认，`--mode=local`） | Server Mode（分布式服务端，`--mode=server`） |
| --- | --- | --- |
| 启动命令 | `evalreel eval` 或 `evalreel serve` | 控制面 `evalreel serve --mode=server` + 计算节点 `evalreel worker` |
| 外部依赖 | 无（单个编译后的 Go 二进制文件直接运行） | MySQL 8 + Redis 7（通过 Docker Compose 一键拉起） |
| 状态存储（`Store`） | `SQLiteStore`（WAL 模式 + 单连接串行写） | `MySQLStore`（连接池并发读写 + 幂等键 + 条件原子状态更新） |
| 任务分发与排队 | `LocalDispatcher` + 内存 `MemoryQueue` + 协程池 | `AsynqDispatcher`（API 投递）+ `AsynqServer`（多 Worker 抢占消费） |
| 速率限制（`Limiter`） | `LocalLimiter`（单进程令牌桶 `rate.Limiter`） | `RedisLuaLimiter`（多 Worker 共享同一个目标模型的 RPM 配额） |
| 实时进度推送（`ProgressBus`） | 内存 `ProgressBroker`（进程内 Go channel 广播） | `RedisProgressBus`（Worker 发布到 Redis Pub/Sub，Server 转发给 SSE） |

### 1.3 新增 Go 第三方依赖清单

1. `gorm.io/driver/mysql`：Gorm 官方 MySQL 数据库驱动，复用现有结构体定义对接 MySQL 8。
2. `github.com/redis/go-redis/v9`：Go 社区主流 Redis 客户端库，用于执行 Lua 全局限流脚本与 Pub/Sub 进度订阅。
3. `github.com/hibiken/asynq`：基于 Redis 的 Go 分布式任务队列库，原生支持任务持久化、租约保活、失败延时重试与死信队列管理。
4. `github.com/prometheus/client_golang`：Prometheus 官方 Go 客户端，用于定义并在 `/metrics` 暴露运行指标。
5. `go.opentelemetry.io/otel`（含 `sdk/trace`、`exporters/stdout/stdouttrace`、`exporters/otlp/otlptrace/otlptracehttp`）：OpenTelemetry 官方 Go SDK，负责生成符合标准格式的链路 Span 并导出。
6. `github.com/alicebob/miniredis/v2`：纯 Go 实现的内存版模拟 Redis 服务端，用于在没有真实 Docker Redis 的单元测试里直接跑通 Lua 脚本和 Pub/Sub 竞态测试。

### 1.4 v3.0 增量目录结构

```text
EvalReel/
├── Dockerfile                         # 多阶段构建单个 Go 运行镜像（对齐 go.mod 的 Go 1.27 版本）
├── docker-compose.yml                 # 多容器编排：mysql、redis、mockllm、server、2x worker 及可选监控
├── deploy/
│   └── prometheus.yml                 # Prometheus 抓取 server / worker 指标配置（observability profile 使用）
├── cmd/evalreel/
│   ├── main.go                        # 扩展 worker 子命令、全局日志与 OTel 初始化参数
│   ├── eval.go                        # serve 支持 --mode=local|server 切换与新增 /api/queues/stats
│   └── worker.go                      # 新增：evalreel worker 分布式消费节点入口
├── internal/
│   ├── observability/                 # Step 4: 日志、监控指标、OTel 导出与 W3C traceparent 解析
│   │   ├── logger.go                  # log/slog JSON/Text 初始化与上下文字段注入
│   │   ├── metrics.go                 # Prometheus 指标注册表与代理/调度统计打点
│   │   ├── otel.go                    # OpenTelemetry TracerProvider（none|stdout|otlp）
│   │   └── traceparent.go             # W3C traceparent 生成/解析与 span_id 提取
│   ├── store/
│   │   ├── mysql.go                   # Step 2: MySQLStore 实现（幂等建单、条件状态更新、事务收口）
│   │   └── spans.go                   # Step 5: TraceSpan 模型与父子 Span 持久化方法
│   ├── scheduler/
│   │   ├── dispatcher.go              # Step 3: Dispatcher 与 ProgressBus 抽象及本地实现
│   │   ├── asynq_dispatcher.go        # Step 3: 基于 hibiken/asynq 的分布式投递器
│   │   ├── redis_limiter.go           # Step 3: 基于 Redis Lua 脚本的分布式令牌桶限流器
│   │   └── redis_progress.go          # Step 3: 基于 Redis Pub/Sub 的跨进程进度总线
│   └── worker/
│       └── worker.go                  # Step 3: Asynq Handler、重试/死信控制与触发收口报告生成
├── docs/
│   ├── 05_v3_detailed_design.md       # 本文档
│   └── 06_v3_benchmark_and_pprof.md   # Step 6: MySQL EXPLAIN 对比与分布式 pprof 压测报告
└── scripts/
    ├── multi_agent.py                 # Step 5: 演示 Planner -> Coder -> Reviewer 透传 traceparent
    ├── mysql_explain_bench.go         # Step 2: 20 万~50 万行造数与 EXPLAIN 执行计划验证脚本
    └── dist_loadtest.go               # Step 6: 面向 Docker Compose 集群的并发评测压测脚本
```

---

## 2. Docker Compose 多容器编排设计（Step 1）

### 2.1 容器拆分原则与多阶段构建

v3.0 外部依赖全部通过 Docker Compose 运行，遵循“每个独立服务占用一个独立容器”的隔离方式，避免把 MySQL、Redis 和 Go 进程打包进同一个容器导致升级和重启互相牵连。

根目录提供多阶段 `Dockerfile`：
1. **构建阶段（Builder）**：基于对应 Go 版本的官方镜像，拷贝 `go.mod` 与 `go.sum` 预先下载依赖（利用 Docker 构建缓存），设置 `CGO_ENABLED=0` 静态编译 `evalreel` 可执行文件。
2. **运行阶段（Runtime）**：基于极简 `alpine` 镜像，仅安装 `ca-certificates` 和 Python 运行基础环境（供可选测试），拷贝编译好的单一 `evalreel` 二进制文件，镜像体积控制在几十 MB 级别。

### 2.2 `docker-compose.yml` 服务清单

| 服务名 | 镜像 / 构建方式 | 核心配置与职责 | 健康检查 / 启用条件 |
| --- | --- | --- | --- |
| `mysql` | `mysql:8.0` | 设置 UTF8MB4 字符集、持久化数据卷 `mysql_data`、初始库名 `evalreel` | `mysqladmin ping -h 127.0.0.1` |
| `redis` | `redis:7-alpine` | 开启 AOF 持久化（`--appendonly yes`，防止容器重启丢失队列任务）与卷 `redis_data` | `redis-cli ping` |
| `mockllm` | 本地镜像 | 运行 `evalreel mockllm --listen :9090 --ttft 500ms --interval 100ms --chunks 30 --fault-429-rate 0.05` | 供压测与离线联调直接访问 |
| `server` | 本地镜像 | 运行 `evalreel serve --mode=server --listen :8081`，连接 `mysql` 和 `redis` 依赖就绪后启动 | 暴露 `8081` 端口（API + `/metrics`） |
| `worker` | 本地镜像 | 运行 `evalreel worker --concurrency 20`，默认通过 `deploy.replicas: 2` 启动 2 个实例，各暴露指标与可选 `pprof` 端口 | `depends_on` 等待 `mysql` 与 `redis` 健康后启动 |
| `prometheus` | `prom/prometheus` | 加载 `deploy/prometheus.yml` 拉取 `server` 与 `worker` 指标 | 放在 `observability` profile，按需启动 |
| `jaeger` | `jaegertracing/all-in-one` | 接收 `server`、`worker`、`proxy` 发出的 OTLP HTTP 链路数据并提供 UI 检索 | 放在 `observability` profile，按需启动 |

说明：`server` 和 `worker` 会挂载同一份读取型工作目录（如 `./testdata`、`./reports`）或通过环境变量配置，保证容器内的 Worker 能读取配置文件里指定路径的数据集并落盘 HTML 文件。

---

## 3. MySQL 状态机、幂等键与索引验证设计（Step 2）

### 3.1 兼容 SQLite 与 MySQL 的表结构升级

v2.0 的 `internal/store/models.go` 已经使用了标准结构体字段，迁移到 MySQL 8 时有三个具体的数据库兼容性问题需要在本阶段一并修正（同时保证 SQLite 单元测试与老库自动迁移无感兼容）：

1. **`eval_runs` 新增幂等键与排序索引**：
   - 增加 `IdempotencyKey *string`（标签 `gorm:"size:128;uniqueIndex"`）。之所以使用指针类型 `*string` 传递 `nil`（对应数据库 `NULL`）并在未传请求头时留空，是因为 MySQL 唯一索引允许存在多条 `NULL` 记录；如果直接用空字符串 `""`，第二个不带幂等键的普通提交就会立刻触发唯一索引冲突报错。
   - 为 `CreatedAt` 增加索引 `index:idx_runs_created_at`，解决 `ListRuns` 按时间倒序分页时的全表扫描与临时文件排序（`Using filesort`）问题。
2. **大文本字段由 `type:text` 升级为 `type:longtext`**：
   - MySQL 中 `TEXT` 类型最大容量仅 64 KB。在 v2.0 实测中，仅 10 道题目的离线 `report.html` 就达到了 33 KB；题目数量达到 50 题或包含较多响应文本时，写入 `eval_reports.html_content` 会直接超出 64 KB 限制。
   - 将 `eval_runs.config_json`、`eval_tasks.request_snapshot` / `reasoning_text` / `response_text`、`eval_scores.reason` / `details_json` 以及 `eval_reports.summary_json` / `html_content` 统一标为 `gorm:"type:longtext"`（SQLite 驱动会将 `longtext` 识别为标准 `TEXT` 亲和类型，不影响单机版行为）。
3. **新增 `trace_spans` 表（记录多 Agent 嵌套调用链节点）**：

| 字段名 | 类型与长度 | 索引 | 说明 |
| --- | --- | --- | --- |
| `span_id` | `varchar(32)` | 主键 | 当前请求或子调用的 16 位十六进制节点编号 |
| `parent_span_id` | `varchar(32)` | 普通索引 | 上级调用方节点编号；顶层根节点为空 |
| `trace_id` | `varchar(64)` | `idx_spans_trace_id` | 贯通一次完整 Agent 任务的全局链路编号 |
| `run_id` | `varchar(64)` | `idx_spans_run_id` | 关联的评测总任务编号（单跑代理时可为空） |
| `task_id` | `varchar(96)` | `idx_spans_task_id` | 关联的子任务编号 |
| `agent_name` | `varchar(64)` | - | 由 `X-EvalReel-Agent-Name` 传入的角色名，如 `planner` |
| `operation` | `varchar(64)` | - | 操作类型，如 `agent_step`、`llm_chat` |
| `model` | `varchar(64)` | - | 请求的模型名 |
| `start_time_ms` | `bigint` | - | 相对起点或 Unix 毫秒开始时间，用于瀑布图排序 |
| `duration_ms` | `bigint` | - | 节点总耗时（毫秒） |
| `ttft_ms` | `bigint` | - | 首字延迟（毫秒） |
| `input_tokens` | `int` | - | 输入词元数 |
| `output_tokens` | `int` | - | 输出词元数 |
| `status` | `varchar(24)` | - | `ok`、`error`、`chaos_injected`、`aborted` |
| `error_msg` | `longtext` | - | 失败时的错误摘要 |

### 3.2 多 Worker 并发写库防重与原子收口

在单机版中只有一个协程池调度；进入 Server Mode 后，2 个以上独立 Worker 容器会并发回写同一张 `eval_runs` 与 `eval_tasks` 表，需要通过数据库条件更新保证状态精准：

1. **子任务终态防重复累加**：
   在 `UpdateTaskResult` 事务内，执行带前置状态条件的更新：`UPDATE eval_tasks SET status = ?, ... WHERE id = ? AND status IN ('pending', 'running')`。只有真正抢到从进行态变为终态（`RowsAffected == 1`）的这一次更新，才执行 `eval_runs.done_tasks = done_tasks + 1`（失败时同步 `+1` `failed_tasks`）。即使某个 Worker 崩溃导致 Asynq 租约过期并由另一个 Worker 重跑，只要前一次已经写成功，后一次不会把总进度加超。
2. **整场评测原子收口（保证全集群只生成一次战报）**：
   每次子任务完成后，Worker 查询对应 `EvalRun`。当发现 `done_tasks >= total_tasks` 且状态仍为 `running` 时，执行一条带条件的状态抢占更新：
   `UPDATE eval_runs SET status = 'completed', finished_at = ? WHERE id = ? AND status = 'running' AND done_tasks >= total_tasks`
   所有并发结束的 Worker 中，数据库行锁确保有且仅有一个节点的 `RowsAffected == 1`。拿到收口权的节点负责调用 `report.Aggregate` 与 `report.RenderHTML`，把战报存入 `eval_reports`（可选同步写一份本地文件），并通过 `ProgressBus` 广播一条 `status: completed` 事件。
   - **Store 接口增量**：在 `internal/store/store.go` 增加两个方法 `TryFinalizeRun(ctx context.Context, runID string, finishedAt time.Time) (bool, error)` 和 `FindRunByIdempotencyKey(ctx context.Context, key string) (*EvalRun, error)`，以及 Span 存取方法 `SaveSpans` / `ListSpansByTraceID`，同时在 `SQLiteStore` 与 `MySQLStore` 中实现，保证测试一致。

### 3.3 `POST /api/runs` 幂等键防重流程

客户端或 CI 脚本发起评测时可在请求头带上 `Idempotency-Key: <唯一字符串>`：
1. Gin 处理器先读取请求头；若存在该键，先调用 `Store.FindRunByIdempotencyKey` 查询是否已经创建过对应 `EvalRun`。如果存在，直接返回 `200 OK` 并带上原有的 `run_id`、`status`、`total_tasks` 与 `"idempotent_replay": true` 标记。
2. 若查询未命中，则正常构造 `EvalRun` 和 `EvalTask` 并调用 `Store.CreateRun`。若并发的两个完全相同请求同时越过第 1 步，MySQL 唯一索引会拦截第二个写入，服务端捕获唯一键冲突错误（MySQL Error 1062 / SQLite UNIQUE constraint）后回查一次原记录并返回给调用方，防止重复建单和重复扣费。

### 3.4 MySQL 索引与 `EXPLAIN` 实测验证方案

在 `scripts/mysql_explain_bench.go` 中编写独立的基准验证脚本：
1. 向本地 Docker MySQL 批量插入 30 万条 `eval_tasks` 模拟数据（分布在 500 个 `run_id` 中，包含 `pending`/`running`/`success`/`failed` 混合状态）。
2. 使用数据库会话连接执行 `EXPLAIN` 与 `EXPLAIN ANALYZE` 检验三类高频核心查询：
   - 断点捞取未完成任务：`SELECT * FROM eval_tasks WHERE run_id = ? AND status IN ('pending', 'running') ORDER BY trial_index ASC, id ASC`（验证命中联合索引 `idx_run_status`）；
   - 汇总战报拉取全量任务：`SELECT * FROM eval_tasks WHERE run_id = ? ORDER BY id ASC`；
   - 控制面历史任务列表：`SELECT * FROM eval_runs ORDER BY created_at DESC LIMIT 20`（验证命中 `idx_runs_created_at`，消除全表 `filesort`）。
3. 将有无索引对比的扫描行数（`rows`）和实际执行毫秒数写入实测报告文档。

---

## 4. Asynq 分布式队列、Redis Lua 限流与跨进程进度总线（Step 3）

### 4.1 为什么抽象 `Dispatcher` 而不是硬套 `Queue.Pop`

v2.0 定义的 `Queue` 接口提供阻塞式的 `Push` 和 `Pop`，这非常适合单机内存里的 `chan` 模型。而工业级队列 `hibiken/asynq` 采用的是“服务端推给注册好的 `Handler` 回调函数”的托管模型：Asynq 内部维护 Redis 阻塞列表、任务租约心跳（防止 Worker 执行一半断电导致任务永久卡死）、自动延时重试队列和死信归档列表。如果强行在 `asynq` 外面硬套一层阻塞 `Pop()`，会丢掉租约确认与自动重试机制。

因此，v3.0 在 `internal/scheduler/dispatcher.go` 向上新增一层轻量的任务分发抽象：

```go
type Dispatcher interface {
    DispatchRun(ctx context.Context, runID string, tasks []store.EvalTask, cfg config.EvalConfig) error
    Close() error
}
```

- **Local Mode**：`LocalDispatcher` 直接沿用 v2.0 写好的 `Scheduler.Run`（底层走 `MemoryQueue` + `LocalLimiter` + 内存 `ProgressBroker`），原有单机体验零改动。
- **Server Mode（API 控制面）**：`POST /api/runs` 落库 `EvalRun` 与 `EvalTask` 后，调用 `AsynqDispatcher.DispatchRun`，只把极轻量的任务索引载荷 `{"run_id": "...", "task_id": "..."}` 推入 Redis（通过 `asynq.TaskID(task.ID)` 防止重复入队，`asynq.MaxRetry(cfg.MaxRetries)` 透传配置的重试上限），50 毫秒内返回给客户端 `202 Accepted`。
- **Server Mode（Worker 计算节点）**：新增独立命令 `evalreel worker`，启动 `asynq.Server` 并发消费队列中的任务。收到载荷后从 MySQL 读出该条 `EvalTask` 与对应 `EvalRun.ConfigJSON`，先走 `RedisLuaLimiter.Wait` 扣减目标配额，再执行 `runner` 和 `scorer`。

### 4.2 失败重试与死信队列处理策略

Worker 处理回调函数里的错误分流遵循严格区分“可重试”与“不可重试”原则：

1. **可重试错误（`RetryableError`，如 429 限流、5xx 服务端错、网络抖动超时）**：原样把错误返回给 `asynq`，同时自定义 `asynq.Config.RetryDelayFunc`：若报错里携带上游返回的 `Retry-After` 秒数则优先按该时长延时重投，否则按指数退避等待。
2. **不可重试错误（如配置无效、脚本参数缺失、JSON 解析错误）**：用 `fmt.Errorf("%w: %v", asynq.SkipRetry, err)` 包装后返回，Asynq 不再浪费次数重试，立刻将该任务置为归档并交由回调写为 `TaskFailed`。
3. **最终进入死信队列后的状态闭环**：在 `asynq.Config.ErrorHandler` 中监听当某条任务彻底达到最大重试次数（或被 `SkipRetry`）时，将数据库中对应的 `eval_tasks.status` 标记为 `failed`、递增总任务的 `done_tasks` 与 `failed_tasks` 并检查是否满足 `TryFinalizeRun` 收口条件；该失败任务本身仍保留在 Asynq 的 Archived（死信）队列里。
4. **队列可观测接口**：在 Gin 接口层新增 `GET /api/queues/stats`（调用 `asynq.Inspector`），实时返回当前排队数（`pending`）、执行中（`active`）、等待重试（`retry`）和死信数量（`archived`）。

### 4.3 Redis Lua 分布式令牌桶限流器（`RedisLuaLimiter`）

`RedisLuaLimiter` 直接实现现有的 `scheduler.Limiter` 接口（`Wait(ctx context.Context, targetName string) error`），让多个独立容器里的 Worker 共享同一把模型账号的每分钟请求配额：

- **Key 设计**：`evalreel:ratelimit:<target_name>`，使用 Redis Hash 结构存储两个字段：`tokens`（当前剩余小数令牌）和 `last_ms`（上次补充时间戳），并设置 `PEXPIRE` 自动清理长期不用的限流 Key。
- **Lua 脚本原子逻辑**：
  1. 在脚本内部调用 `redis.call("TIME")` 获取 Redis 服务端统一时间戳（毫秒），避免多个容器或宿主机系统时钟不一致造成配额误差。
  2. 计算距离 `last_ms` 经过的时间，按 `rate_per_ms = rpm / 60000.0` 补充令牌并封顶 `burst`（默认为 `max(1, min(5, rpm/10))` 或可配置）。
  3. 如果 `tokens >= 1.0`，则扣减 `1.0` 并写回 `tokens` 和 `last_ms`，返回 `{1, 0}`；否则不扣减，计算补满 1 枚令牌所需等待的毫秒数 `wait_ms`，返回 `{0, wait_ms}`。
- **Go 侧等待与取消**：`Wait` 方法循环执行该脚本；返回 `0` 时加上最多几十毫秒的随机抖动（Jitter，避免多台 Worker 在同一毫秒齐刷刷醒来撞墙）后挂起等待，同时严格监听 `ctx.Done()`，评测取消或超时时立刻退出阻塞。

### 4.4 跨进程实时进度总线（`ProgressBus`）

在 Server Mode 下，执行任务的是 `worker` 容器，持有客户端浏览器或脚本 SSE 长连接的是 `server` 容器，两者不在同一个进程内存空间。为此抽象统一接口：

```go
type ProgressBus interface {
    Publish(ctx context.Context, ev ProgressEvent) error
    Subscribe(ctx context.Context, runID string) (<-chan ProgressEvent, func(), error)
}
```

- Local Mode 的适配器直接封装原有内存版 `ProgressBroker`。
- Server Mode 使用 `RedisProgressBus`：Worker 每完成一条子任务后向频道 `evalreel:progress:<run_id>` 发送序列化 JSON；`server` 收到前端 `GET /api/runs/:id/events` 请求后订阅该 Redis 频道并转发成 SSE 事件流。如果客户端连上时任务已经结束或进度有领先，先由数据库初始快照兜底一次状态后再切换流式订阅，确保零丢状态。

---

## 5. 可观测性与多 Agent 白盒调用树（Step 4 & Step 5）

### 5.1 日志、指标与 OpenTelemetry 底座（`internal/observability`）

1. **统一结构化日志（`log/slog`）**：支持命令行与环境变量 `--log-format=text|json` 与 `--log-level=info|debug`，容器默认输出单行 JSON 方便收集，在每个任务处理上下文固定附上 `service`、`run_id`、`task_id`、`trace_id`。
2. **Prometheus 指标（暴露于 `GET /metrics`）**：
   - `evalreel_proxy_requests_total{model, status, chaos}`：代理转发请求累计计数；
   - `evalreel_proxy_active_streams`：当前活跃流式长连接数（用于直观监控长连接泄露）；
   - `evalreel_llm_ttft_ms`：大模型首字延迟直方图（区分代理观测与批量评测执行）；
   - `evalreel_worker_tasks_total{target, status}` 与 `evalreel_worker_task_duration_ms`：Worker 执行结果统计与耗时分布；
   - `evalreel_limiter_wait_ms{target}`：因触发 RPM 限流而阻塞等待的耗时分布。
3. **OpenTelemetry GenAI 导出**：
   - 通过 `--otel-exporter=none|stdout|otlp` 与 `--otel-endpoint` 控制开关（无收集器时也可设为 `stdout` 在控制台直观检查 Span JSON）。
   - 为每次模型调用和子 Agent 步骤写入符合 OpenTelemetry GenAI 语义规范的核心属性：`gen_ai.system`、`gen_ai.request.model`、`gen_ai.response.model`、`gen_ai.usage.input_tokens`、`gen_ai.usage.output_tokens`、`gen_ai.response.finish_reasons`。

### 5.2 多 Agent 白盒嵌套调用树（W3C `traceparent` 协议串联）

在 v2.0 结束时我们约定把“子 Agent A 在第几秒调了子 Agent B 的嵌套调用树”放在 v3.0 补齐，其数据流设计覆盖从拉起子进程到生成 HTML 战报的完整链路：

```text
[AgentRunner 启动子进程]
   生成根 trace_id + root_span_id -> 写入环境变量 TRACEPARENT / EVALREEL_RUN_ID / EVALREEL_TASK_ID
        │
        ▼
[外部多 Agent 脚本 (如 scripts/multi_agent.py)]
   ├─ Planner 步骤生成 span_p（parent = root_span_id），带头 traceparent + X-EvalReel-Agent-Name: planner 调 EvalReel Proxy
   ├─ Coder 步骤生成 span_c（parent = span_p），带头调 EvalReel Proxy
   └─ Reviewer 步骤生成 span_r（parent = span_p），带头调 EvalReel Proxy
        │
        ▼
[internal/proxy captureMiddleware]
   解析 traceparent 取出 trace_id 与 parent_span_id，生成当前请求 span_id
   -> 写入 TraceRecord (.jsonl.gz) + 异步导出 OTel Span + 若挂载 Store 则写入 trace_spans 表
        │
        ▼
[internal/report 汇总与渲染]
   按 trace_id 查出所有 TraceSpan -> 组装父子树结构（自动防环 + 孤儿节点挂接根节点）
   -> 在 report.html 单题对比弹窗/折叠区渲染时间轴瀑布图
```

具体落地细节：
1. **向后兼容 W3C `traceparent` 解析**：格式为 `00-<32位小写十六进制trace_id>-<16位小写十六进制parent_span_id>-<2位flags>`。如果请求没有带该请求头，`proxy` 保持 v1.0 原有随机生成逻辑，完全不影响普通单 Agent 或纯 SDK 调用。
2. **无感接入外部 Agent 脚本**：`AgentRunner` 在构造子进程环境变量时自动注入 `TRACEPARENT`、`EVALREEL_RUN_ID` 和 `EVALREEL_TASK_ID`，并把根 `trace_id` 存进 `eval_tasks.trace_id`；`proxy` 侧若检测到 `X-EvalReel-Run-ID` / `X-EvalReel-Task-ID` 或从上下文传回时一并关联落库。
3. **新增极简验证脚本 `scripts/multi_agent.py`**：约 90 行 Python 代码，使用标准库 `os.environ.get("TRACEPARENT")` 读出根上下文，演示主调度器 `planner` 先调大模型拆分任务、再派生 `coder` 与 `reviewer` 两个角色依次发请求，每次只改请求头里的当前 `parent_span_id` 与 `X-EvalReel-Agent-Name`。
4. **纯离线调用树瀑布图**：在 `internal/report` 汇总时读取 `trace_spans`，计算出每个节点的相对启动偏移（`offset_ms = span.start_time_ms - min_start_ms`）和宽度比例，直接用内嵌 SVG 与折叠 DOM 渲染在 `report.html` 的单题差异明细卡片中，断网双击同样可以逐层展开查看哪个子 Agent 慢、哪一步报错或花了多少词元。

---

## 6. 真实延迟流式压测与 `pprof` 分析方案（Step 6）

### 6.1 增强 `mockllm` 贴近真实慢推理负载

禁止用“立刻返回 200 OK”的假接口刷虚高 QPS 数据。v3.0 在 `internal/mockllm` 与 `evalreel mockllm` 命令行新增两个参数：
- `--chunks`（默认按文本段数，压测可设为 `30`）：按 `--interval 100ms` 稳定输出指定数量的分块 SSE 数据，配合 `--ttft 500ms`，构造出单请求真实持续约 3.5 秒的流式长连接，把压力真实传导到 Go 协程池、HTTP 连接池和内存缓冲区。
- `--fault-429-rate`（0.0~1.0，例如 `0.05`）：按 5% 概率在首包前随机返回 HTTP 429 并附带 `Retry-After: 1` 响应头，用于在压测期间同步检验跨 Worker 的 Asynq 退避重试和状态防重能力。

### 6.2 多容器压测拓扑与验收红线

1. **压测拓扑**：执行 `docker compose up -d --build` 拉起 1 个 `mysql`、1 个 `redis`、1 个 `mockllm`（配置 500ms TTFT + 3s 流式 + 5% 429）、1 个 `server` 与 2 个 `worker`（每个 Worker 设 `concurrency=25`，集群总并发 50）。
2. **压测脚本（`scripts/dist_loadtest.go`）**：以并发客户端向 `POST /api/runs`（带与不带 `Idempotency-Key` 混合）持续提交评测任务（总计 600–1,000 条流式子任务，涵盖多目标共享 `rpm_limit` 配额）。
3. **必须记录进 `docs/06_v3_benchmark_and_pprof.md` 的实测数据项**：
   - **正确性与幂等**：提交总数、最终 `completed` 状态正确率、子任务零重复计费与唯一战报生成验证；
   - **分布式限流精度**：对设定了如 `300 RPM`（集群合计）的目标模型，统计 2 个 Worker 实际落到 `mockllm` 的每分钟请求速率是否稳定收口在配额上限附近；
   - **资源稳定性（`pprof` 基线对照）**：压测前、压测高峰期、全部任务结束 10 秒后，分别对 `server` 和 `worker` 抓取 `goroutine` 数量与 `heap` `HeapAlloc` 内存，要求结束后协程数回落至基线（零残留流式或 Pub/Sub 监听协程）。

---

## 7. 6 步分阶段实施与 Git 提交路线图

按照每步完成并跑通含 `-race` 竞态检测的单元测试后再提交 Git 的节奏推进：

| 步骤 | 涉及模块 | 核心交付与验收标准 |
| --- | --- | --- |
| **Step 1** | `Dockerfile`、`docker-compose.yml`、`deploy/` | 启动本机 Docker Desktop，完成多阶段镜像构建与 `mysql:8`、`redis:7-alpine` 健康检查拉通。 |
| **Step 2** | `internal/store/mysql.go`、`models.go`、`api/server.go`、`scripts/mysql_explain_bench.go` | `MySQLStore` 全部接口通过集成测试；`idempotency_key`（`*string`）防重通过测试；`longtext` 升级完成；30 万行模拟数据跑通 `EXPLAIN` 索引验证并保留实测输出。 |
| **Step 3** | `internal/scheduler/*`、`internal/worker/*`、`cmd/evalreel/worker.go` | `AsynqDispatcher`、`evalreel worker`、`RedisLuaLimiter`、`RedisProgressBus` 和 `GET /api/queues/stats` 全部落地；基于 `miniredis` 的单测和双 Worker 容器真实抢任务测试全部通过 `-race` 检测；Local Mode 保持默认零依赖并通过回归测试。 |
| **Step 4** | `internal/observability/*`、`cmd/evalreel/` | 接入结构化 `log/slog`、Prometheus `/metrics` 端点、OpenTelemetry GenAI 导出（`none|stdout|otlp`）与 W3C `traceparent` 工具包，全部单元测试含 `goleak` 检查通过。 |
| **Step 5** | `internal/proxy/`、`internal/runner/`、`internal/store/spans.go`、`internal/report/`、`scripts/multi_agent.py` | Proxy 与 AgentRunner 贯通 `traceparent` 与 `trace_spans` 落库；跑通 `scripts/multi_agent.py` 多 Agent 嵌套场景；离线 `report.html` 能折叠展开嵌套调用树与时间轴瀑布图。 |
| **Step 6** | `internal/mockllm/`、`scripts/dist_loadtest.go`、`docs/06_v3_benchmark_and_pprof.md` | 完成 500ms TTFT + 3s 流式 + 5% 429 压测场景的集群长时压测，产出含 `pprof` 内存/协程分析与 MySQL `EXPLAIN` 数据的最终验收报告，全库 `go test -race ./...` 通过并收口提交。 |

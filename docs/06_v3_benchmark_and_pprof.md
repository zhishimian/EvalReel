# EvalReel v3.0 性能基准测试、索引验证与压测实录

本文记录 EvalReel v3.0 在本地 Docker Compose 环境（MySQL 8.0.46 + Redis 7 Alpine + Go 1.27.4）下的三组实测结果，全部数据均来自实际执行输出：

1. **MySQL 30 万行模拟数据 `EXPLAIN ANALYZE` 索引对比**（Step 2）
2. **多 Agent `traceparent` 白盒调用树端到端联调**（Step 5）
3. **双 Worker 容器 50 并发慢流式 + 5% 随机 429 压测与 `pprof` 分析**（Step 6）

---

## 一、MySQL 30 万行模拟数据 `EXPLAIN ANALYZE` 索引验证

测试脚本：[mysql_explain_bench.go](file:///e:/vscode/EvalReel/scripts/mysql_explain_bench.go)  
测试样本：向测试表写入 `500` 场评测总任务（`bench_eval_runs`）和 `300,000` 条子任务记录（`bench_eval_tasks`，单场 600 条，覆盖 `success`/`failed`/`pending`/`running` 状态），建数耗时 `4.686s`。

| 查询场景 | 未建二级索引（阶段 A） | 建立 v3.0 索引后（阶段 B） | 实测变化 |
| --- | --- | --- | --- |
| **Q1：断点续跑捞未完成任务**<br>`WHERE run_id = ? AND status IN ('pending','running')` | `type=ALL`（全表扫描 `300,000` 行）<br>`EXPLAIN ANALYZE` 总耗时：`114.265ms` | `type=range`，命中联合索引 `idx_bench_run_status`（扫描 `200` 行）<br>`EXPLAIN ANALYZE` 总耗时：`12.201ms` | 扫描行数由 30 万行降至 200 行，耗时降至约十分之一 |
| **Q2：战报聚合拉取子任务**<br>`WHERE run_id = ? ORDER BY id ASC` | `type=index`（扫描主索引 `300,000` 行）<br>`EXPLAIN ANALYZE` 总耗时：`115.736ms` | `type=ref`，命中 `idx_bench_run_status`（扫描 `600` 行）<br>`EXPLAIN ANALYZE` 总耗时：`23.065ms` | 扫描行数降至 600 行，耗时降至约五分之一 |
| **Q3：历史评测列表分页**<br>`ORDER BY created_at DESC LIMIT 20`（500 行基表） | `type=ALL`（500 行全扫描）<br>`EXPLAIN ANALYZE` 总耗时：`2.144ms` | `type=ALL`（500 行全扫描）<br>`EXPLAIN ANALYZE` 总耗时：`2.156ms` | 因基表仅 500 行且查询列未全部覆盖索引，MySQL 优化器判定全表排序成本低于回表 |

---

## 二、多 Agent `traceparent` 白盒调用树实测（Step 5）

测试链路：`evalreel eval` 隔离子进程（注入 `TRACEPARENT`）→ [multi_agent.py](file:///e:/vscode/EvalReel/scripts/multi_agent.py)（顺序触发 `planner` → `coder` → `reviewer`）→ `evalreel proxy`（开启 `-db` 写 SQLite `trace_spans` 表）→ `mockllm`。

实际产出报告 `reports/report_run_20261123_205249.html`（大小 `10,960` 字节，总评测耗时 `3306ms`，准确率 `100.0%`）：

| 节点角色 | 嵌套深度 | 相对偏移 | 单步耗时 | 词元（输入/输出） | 瀑布条定位 |
| --- | --- | --- | --- | --- | --- |
| `planner` | `depth=0`（缩进 `0px`） | `+0ms` | `506ms` | `18 / 24` | `left: 0.0%`, `width: 32.0%` |
| `coder` | `depth=1`（缩进 `14px`） | `+548ms` | `506ms` | `18 / 24` | `left: 34.7%`, `width: 32.0%` |
| `reviewer` | `depth=2`（缩进 `28px`） | `+1077ms` | `504ms` | `18 / 24` | `left: 68.1%`, `width: 31.9%` |

说明：`planner` 从代理响应头 `X-EvalReel-Span-ID` 拿到代理为其生成的 `span_id` 后填入下游请求头 `traceparent`，`coder` 再把自身 `span_id` 传给 `reviewer`，最终离线战报直接折叠展示 `调用层级树 (3 跳)`。

---

## 三、分布式集群慢流式 + 5% 随机 429 压测与 `pprof` 分析（Step 6）

### 3.1 压测环境与负载参数

- **集群拓扑**：Docker Compose 启动 `mysql:8.0`、`redis:7-alpine`、`mockllm`、`1 server`（`:8081` API + `:6061` pprof）、`2 worker`（每个 `concurrency=25`，合计 50 并发）。
- **慢流式靶子配置**：`-ttft=500ms -interval=100ms -chunks=20 -fault-429-rate=0.05`（单条成功请求约持续 2.6 秒流式长连接，同时随机对 5% 请求注入带 `Retry-After: 1` 的 HTTP 429）。
- **压测客户端**：运行 [dist_loadtest.go](file:///e:/vscode/EvalReel/scripts/dist_loadtest.go)，并发提交 `10` 场评测，每场 `10` 题 × `3` 遍 = `300` 条流式子任务（附带首场 `Idempotency-Key` 幂等重放检验）。

### 3.2 任务正确率、重试恢复与吞吐结果

| 观测项 | 实测结果 | 说明 |
| --- | --- | --- |
| 提交耗时（10 场共 300 条子任务） | `124ms` | 控制面仅写库后投递轻量子任务 ID 进 Redis |
| 幂等键重放校验 | 通过（`200 OK` + `idempotent_replay=true`） | 相同 `Idempotency-Key` 复用同一 `run_id` |
| 完成场次与战报可用率 | `10 / 10` 完成，`10 / 10` 战报返回 `200 OK` | 每场均由唯一一个 Worker 通过 `TryFinalizeRun` 抢占生成 |
| 子任务成功率与 429 恢复数 | `300 / 300` 成功（`FailedTasks=0`），`14` 条触发 429 后重试恢复 | 实际命中 429 比例约 4.7%，全部经 Asynq 退避重投恢复 |
| 总耗时与集群有效吞吐 | `20.696s` 完成全部 300 条慢流式任务，吞吐 `14.50 tasks/s` | 单 Worker 1 监控指标记录 `success=150`、`error=10`（含 429 重试前报错） |

### 3.3 `pprof` 与 Prometheus 内存及协程回收统计

通过控制面 `:8081/metrics`（`go_goroutines`、`go_memstats_alloc_bytes`）与 `:6061/debug/pprof/goroutine?debug=1` 采样得到：

| 指标 | 压测前基线（Baseline） | 压测高峰（Peak） | 结束后静置 10 秒（Idle +10s） |
| --- | --- | --- | --- |
| Server `go_goroutines` | `17` | `45` | `25`（`pprof goroutine total 21`） |
| Server `HeapAlloc` | `2.58 MB` | `8.84 MB` | `9.41 MB` |
| Worker 1（压测后）`go_goroutines` | — | — | `30` |

**协程剖析明细说明**：压测全部结束后查看 Server `:6061/debug/pprof/goroutine?debug=1` 输出的 `21` 个常驻协程中，有 `10` 个属于 `go-sql-driver/mysql` 连接池内闲置连接的监听协程（`mysqlConn.startWatcher`）、`2` 个属于 `go-redis` 的熔断器清理协程（`CircuitBreakerManager.cleanupLoop`），其余为 Go 运行时及 HTTP 监听底座协程，未见任何 SSE 订阅或代理请求处理协程残留。

---

## 四、全量测试套件验证

在开启 `-race` 竞态检测并连接本地 Docker MySQL 的条件下执行 `go test -race -count=1 ./...`，共 `13` 个含测试文件的功能包（`api`、`chaos`、`mockllm`、`observability`、`proxy`、`recorder`、`report`、`runner`、`scheduler`、`scorer`、`sse`、`store`、`worker`）全部通过，其中 `api`、`observability`、`proxy`、`runner`、`scheduler`、`scorer`、`sse`、`worker` 均启用了 `goleak.VerifyTestMain(m)` 校验零协程泄漏。

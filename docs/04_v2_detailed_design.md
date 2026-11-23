# EvalReel v2.0 详细设计文档

## 1. v2.0 目标、选型与边界

v2.0 在 v1.0 透明流式代理的基础上，补齐**“主动批量跑任务、断点续跑、自动批卷、第 N 步读档横评和离线网页战报”**五个环节，让整个工具从单纯的“录像代理”升级为可独立使用的单机评测平台。

- **推进顺序（顺序 A，从底座往上搭）：**
  1. 先写数据库层（`internal/store`）；
  2. 再写并发调度与限流引擎（`internal/scheduler`）；
  3. 再接好 Gin（Go 语言常用的轻量 Web 接口框架）接口和实时进度推送（`internal/api`）；
  4. 接着实现三类任务执行器（`internal/runner`）；
  5. 然后实现三合一自动批卷流水线（`internal/scorer`）；
  6. 最后汇总指标、生成断网可打开的 `report.html` 并接入命令行（`internal/report` + `cmd/evalreel`）。
- **配置与数据格式约定：** 评测任务配置采用原生 **JSON**（如 `configs/eval.example.json`），题库和轨迹继续沿用每行一条 JSON 的 **JSONL** / `.jsonl.gz` 格式。
- **v2.0 边界约束：** v2.0 全程使用单机零配置底座（SQLite 本地单文件数据库 + 内存通道队列 + 单机限流器），暂不引入 MySQL、Redis 和 Asynq（这些放在 v3.0，通过同一个接口无缝替换），不写前后端分离的管理后台页面，不接入 MCP 协议代理。
- **新增 Go 第三方库：**
  1. `github.com/gin-gonic/gin`：提供创建任务、查询状态、推送实时进度和下载报告的 HTTP 接口。
  2. `gorm.io/gorm`：Go 语言常用的 ORM（把代码里的结构体自动映射成数据库表行的操作库），负责读写任务状态机。
  3. `github.com/glebarez/sqlite`：纯 Go 实现的 SQLite 驱动，不需要调用 C 语言库，打包单文件工具时不会在 Windows 下报编译错。
  4. `golang.org/x/time/rate`：Go 官方扩展库提供的令牌桶限流器，按每分钟请求上限控制发包速度。

### 1.1 v2.0 增量目录结构

在 v1.0 已有目录（`mockllm`、`recorder`、`sse`、`proxy`、`chaos`）的基础上，v2.0 新增以下目录：

```text
EvalReel/
├── configs/
│   └── eval.example.json          # JSON 评测任务配置模板
├── internal/
│   ├── store/                     # Step 1: Store 接口 + Gorm SQLite 4张表实现
│   ├── scheduler/                 # Step 2: Queue/Limiter 接口 + 协程池 + 退避重试
│   ├── api/                       # Step 3: Gin HTTP 路由 + SSE 实时进度推送
│   ├── runner/                    # Step 4: 纯大模型 / 第 N 步读档 / 隔离子进程 Agent
│   ├── scorer/                    # Step 5: 规则匹配 / 二元 AI 裁判 / ExecGrader
│   └── report/                    # Step 6: 指标汇总 + 单文件 report.html 生成
├── testdata/                      # v2.0 标准极简测试集
│   ├── humaneval_mini.jsonl       # 10 道 Python 编程与单元测试题
│   └── toolcall_mini.jsonl        # 10 道工具调用判断与参数校验题
└── scripts/
    ├── mini_agent.py              # 已有：2 个地理算距工具的多轮 Agent
    └── file_agent.py              # 新增：读写本地文件的多轮 Agent
```

---

## 2. 数据库表结构设计（4 张核心表）

为了在 v3.0 平滑切换到 MySQL，这 4 张表全部使用常规字段类型和标准索引，不依赖 SQLite 专属语法：

| 表名 | 作用 | 核心字段与索引设计 |
| --- | --- | --- |
| `eval_runs` | 一次评测总任务 | `id` (主键，如 `run_20261123_001`)、`mode` (`benchmark`/`fork`/`agent`)、`status` (`running`/`completed`/`failed`/`canceled`)、`config_json` (任务配置快照)、`total_tasks`、`done_tasks`、`failed_tasks`、`created_at`、`finished_at` |
| `eval_tasks` | 拆出来的每一条子任务（某道题 × 某个模型 × 第 $k$ 遍） | `id` (主键)、`run_id`、`item_id` (题目编号)、`target_name` (模型或目标名)、`trial_index` (第几遍)、`status` (`pending` → `running` → `success`/`failed`)、`retry_count`、`trace_id`、`request_snapshot` (输入快照 JSON)、`response_text`、`tool_calls_json`、`ttft_ms`、`total_ms`、`input_tokens`、`output_tokens`、`cost_usd`、`error_msg`、`updated_at`。<br>**核心联合索引：** `idx_run_status (run_id, status)`，用于断点续跑时秒级捞出未完成任务。 |
| `eval_scores` | 每条子任务的批卷明细（支持挂多个打分器） | `id` (主键)、`task_id` (关联子任务)、`run_id`、`scorer_name` (如 `rule_tool`、`judge_v1`、`exec_py`)、`passed` (布尔值：是否通过)、`rubric_hash` (裁判标准版本号)、`reason` (原因说明)、`details_json`、`created_at` |
| `eval_reports` | 整场评测汇总结果与离线网页缓存 | `run_id` (主键)、`summary_json` (各模型通过率、`pass@k`、`pass^k`、P50/P95 延迟、总费用)、`html_content` (生成好的完整 HTML 字符串，也可按路径存磁盘)、`created_at` |

### SQLite 并发写库防报错必改点（避开 `SQLITE_BUSY`）
SQLite 是本地单文件数据库，如果 20 个协程同时抢着写同一张表，极易报 `database is locked (SQLITE_BUSY)` 错误。初始化 Gorm 连接时必须同时做两件事：
1. 开启 `WAL`（预写日志）模式：`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`（允许读写共存，等待最多 5 秒）；
2. 设置底层连接池最大连接数为 1：`sqlDB.SetMaxOpenConns(1)`（让 Go 连接池内部自动排队串行写本地文件，彻底消除多协程同时写文件锁冲突）。

---

## 3. JSON 评测任务配置规范（`configs/eval.example.json`）

```json
{
  "name": "toolcall_baseline_vs_qwen_ds",
  "mode": "benchmark",
  "dataset_path": "./testdata/toolcall_mini.jsonl",
  "fork_source": {
    "trace_file": "",
    "trace_id": "",
    "step_index": 0
  },
  "agent": {
    "command": "uv",
    "args": ["run", "--python", "3.12", "--with", "openai", "scripts/file_agent.py"],
    "timeout_sec": 60
  },
  "concurrency": 10,
  "trials": 3,
  "max_retries": 2,
  "targets": [
    {
      "name": "qwen3.8-flash",
      "base_url": "https://ws-uhpyag0mepnt0gk2.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
      "api_key_env": "OPENAI_API_KEY",
      "model": "qwen3.8-flash",
      "rpm_limit": 120,
      "input_price_per_1m": 0.8,
      "output_price_per_1m": 2.7
    },
    {
      "name": "deepseek-v4.1-flash-1017",
      "base_url": "https://ws-uhpyag0mepnt0gk2.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
      "api_key_env": "OPENAI_API_KEY",
      "model": "deepseek-v4.1-flash-1017",
      "rpm_limit": 60,
      "input_price_per_1m": 2.0,
      "output_price_per_1m": 8.0
    }
  ],
  "scorers": [
    {
      "type": "rule",
      "name": "tool_match"
    }
  ],
  "output_dir": "./reports"
}
```

---

## 4. 按顺序 A 拆解的 6 步模块详细设计

### Step 1：`internal/store`（存储接口与 Gorm 状态机）
- **接口定义：**
  ```go
  type Store interface {
      CreateRun(ctx context.Context, run *EvalRun, tasks []EvalTask) error
      ClaimPendingTasks(ctx context.Context, runID string) ([]EvalTask, error) // 捞取 pending 与中断前卡在 running 的任务
      UpdateTaskStatus(ctx context.Context, taskID string, update TaskUpdate) error
      SaveScores(ctx context.Context, scores []EvalScore) error
      GetRun(ctx context.Context, runID string) (*EvalRun, []EvalTask, []EvalScore, error)
      ListRuns(ctx context.Context, limit int) ([]EvalRun, error)
      SaveReport(ctx context.Context, report *EvalReport) error
      Close() error
  }
  ```
- **断点续跑机制：** 当服务异常退出重启或用户执行 `evalreel eval -resume <run_id>` 时，`ClaimPendingTasks` 查询 `WHERE run_id = ? AND status IN ('pending', 'running')`，只把未完成的任务重新塞进队列；已经变成 `success` 的子任务直接跳过，不会重复花钱调接口。

### Step 2：`internal/scheduler`（任务队列、协程池与限流重试）
- **双核心接口（v3.0 切换 Redis/Asynq 时只换实现）：**
  ```go
  type Queue interface {
      Push(ctx context.Context, taskIDs []string) error
      Pop(ctx context.Context) (string, bool, error)
      Close() error
  }

  type Limiter interface {
      Wait(ctx context.Context, targetName string) error
  }
  ```
- **v2.0 单机实现：**
  1. **队列 + Worker Pool：** 使用带缓冲的 Go `chan string` 保存待执行子任务编号，启动 `concurrency`（例如 10）个后台协程共同从通道领取任务。
  2. **按目标独立限流：** 为每个被测模型单独维护一个 `rate.NewLimiter(rate.Limit(float64(rpm)/60.0), burst)`。这样跑 `qwen3.8-flash`（120 RPM）和 `deepseek-v4.1-flash-1017`（60 RPM）时互不卡脖子。
  3. **超时与指数退避重试：** 每次调用绑定 `context.WithTimeout`，遇到 HTTP 429、5xx 或网络抖动时，按 `1s` → `2s` → `4s` 等待后重试（若响应头带了 `Retry-After` 则优先按该秒数等待），超过 `max_retries` 才标为 `failed`。
  4. **优雅停机与实时事件通道：** 每完成一条子任务，同时往轻量的 `ProgressBroker`（订阅发布通道）发送进度事件 `{done, total, failed, current_tps}`，供 Gin 的 SSE 接口实时推给前端。

### Step 3：`internal/api`（Gin HTTP 接入层与 SSE 看板接口）
- **核心路由：**
  - `POST /api/runs`：校验 JSON 配置（如缺少 `api_key_env` 环境变量立刻报 400），调用 `Store.CreateRun` 写入数据库，把任务扔进后台调度器，**50 毫秒内返回 `{"run_id": "..."}`**。
  - `GET /api/runs`：列出历史评测记录、模型和通过率摘要。
  - `GET /api/runs/:id`：返回单次评测的完整任务统计与每道题详情。
  - `GET /api/runs/:id/events`：SSE（Server-Sent Events，服务端向客户端单向推送数据的长连接）接口，绑定 Gin `c.Stream(...)`，每秒或每完成一题推送实时进度 JSON，直到任务结束或客户端断开。
  - `GET /api/runs/:id/report`：直接返回生成好的 `report.html` 网页文件下载。

### Step 4：`internal/runner`（三合一任务执行器）
定义统一接口：`Execute(ctx context.Context, target TargetConfig, task EvalTask) (RunOutput, error)`。
1. **纯大模型流式执行器（`BenchmarkRunner`）：**
   - 直接向目标模型发 `stream: true` + `stream_options: {"include_usage": true}` 请求，**直接复用 v1.0 写好的 `sse.ParseStream`** 提取 `ReasoningText`、`ContentText`、`ToolCalls`、`TTFTReasoningMs`、`TTFTAnswerMs` 和真实 `usage`，按单价算出花费。
2. **第 N 步读档分叉执行器（`ForkRunner`，核心差异化功能）：**
   - 读取 v1.0 录制好的 `.jsonl.gz` 文件，定位到指定 `trace_id`（若是一轮接一轮的多条记录，可按序读出前 $N-1$ 轮的 `request_body` 与 `tool_calls`/回复上下文），组装成第 $N$ 步的完整 `messages` 和 `tools`；
   - 随后用多个候选模型各跑 $k$ 遍，对比不同模型在完全相同历史上下文下的下一步工具决策差异（Diff）。
3. **隔离子进程 Agent 执行器（`AgentRunner`）：**
   - 为每条子任务在系统临时目录创建独立隔离文件夹（例如 `os.MkdirTemp("", "evalreel-agent-*")`），通过命令行工作目录 `cmd.Dir = workDir` 拉起外部 Agent（如 `scripts/file_agent.py`），并注入环境变量 `OPENAI_BASE_URL`、`OPENAI_API_KEY`、`OPENAI_MODEL`（也可以直接把 `OPENAI_BASE_URL` 指回 v1.0 代理以录制过程）；
   - **进程树超时强杀：** Windows 下使用作业对象（Job Object）或 `taskkill /T /F /PID`，Linux 下使用 `Setpgid: true` 设置进程组并在超时时 `syscall.Kill(-pid, syscall.SIGKILL)`，确保 Agent 衍生出的子进程一并清理干净。

### Step 5：`internal/scorer`（三合一自动批卷流水线）
定义统一接口：`Score(ctx context.Context, item DatasetItem, out RunOutput) (EvalScore, error)`。支持在配置里按数组组合多个打分器：
1. **规则快判（`RuleScorer`）：**
   - 文本规则：精确匹配、子串包含、正则表达式、合法 JSON 校验；
   - 工具调用规则：检查是否调用了期望的 `tool_name`、必填参数键名与取值是否匹配、以及在 `must_not_call: true` 题目下模型是否遵守规则没有乱调工具。
2. **二元 AI 裁判（`JudgeScorer`）：**
   - 采用工业界最佳实践：**强制二元判定 `{"passed": true/false, "checklist": [...], "reason": "..."}`**，避免不稳定的 1–10 浮点打分；
   - 要求裁判模型先输出检查条目的分析，再输出布尔结果；计算并记录裁判 Prompt 和评分标准的 SHA-256 哈希（`rubric_hash`），改动评判标准后可追溯。
3. **外部脚本判分器（`ExecScorer` / `ExecGrader`）：**
   - 针对代码生成题（如 `humaneval_mini.jsonl`）或修改文件类 Agent 任务（如 `file_agent.py` 写完代码后），将模型生成的函数代码与题目里的 `test` 单元测试拼成一个临时 `.py` 文件（或直接在隔离工作目录里），启动 `python` 执行，**退出码为 0 即判 `passed: true`**，非 0 则保存 `stderr` 报错信息判 `passed: false`。

### Step 6：`internal/report`（指标汇总与单文件 `report.html`）
- **核心统计指标计算：**
  1. **单次通过率（Accuracy）：** 全部子任务中通过的比例；
  2. **`pass@k`（至少成功 1 次的概率）：** 当 `trials = k` 时，同一道题 $k$ 次尝试中至少有 1 次通过的题目占比；
  3. **`pass^k`（连续 $k$ 次全部成功的稳定性）：** 同一道题 $k$ 次尝试**每次都通过**的题目占比（直接揪出“偶尔蒙对但线上不可靠”的模型）；
  4. **延迟分布（P50 / P95）：** 将耗时从小到大排序后取第 50% 分位和第 95% 分位（反映绝大部分请求在几秒内完成）、平均思考首字延迟、平均正文首字延迟、平均 TPS 和总估算花费（元/美元）。
- **单文件离线 `report.html` 模板：**
  - 用 Go 标准库 `html/template` 把统计 JSON 直接内联写入 HTML `<script>` 变量，**不依赖任何外网 CDN 脚本**（确保断网或发给同事双击即可打开）；
  - 页面包含三部分：顶部模型横向对比总表、柱状延迟/通过率对比图（纯内联 SVG 绘制，零第三方 JS 依赖）、底部逐题并排差异对比（Diff 面板：左列模型 A 回答与工具参数，右列模型 B 回答与工具参数，支持按“仅看失败/分歧题目”过滤）。

---

## 5. 测试夹具对应表与分步验收清单

已经配套准备好以下两套 10 题标准数据集与两个本地 Agent 脚本，v2.0 每写完一步即可用真模型实测：

| 测试素材 | 位置 | 对应测试的 v2.0 核心能力 |
| --- | --- | --- |
| **10 道代码单元测试题** | `testdata/humaneval_mini.jsonl` | 验证 `BenchmarkRunner` + `ExecScorer`（Python 退出码客观判题）+ `pass@k` / `pass^k` |
| **10 道工具调用题** | `testdata/toolcall_mini.jsonl` | 验证 `RuleScorer`（工具名/参数校验 + 负样本不乱调工具）+ 第 $N$ 步读档对比 `qwen3.8-flash` vs `deepseek-v4.1-flash-1017` |
| **2 轮地理工具 Agent** | `scripts/mini_agent.py` | 验证 v1.0 录制 `.jsonl.gz` 后，由 v2.0 `ForkRunner` 读取第 2 步上下文横评不同模型 |
| **本地文件读写 Agent** | `scripts/file_agent.py` | 验证 `AgentRunner` 的临时目录隔离、超时进程组强杀以及工作目录产物校验 |

### 分步开发与自检命令
- **Step 1 (`store`)：** 跑通建表、写入 100 条子任务、模拟中途崩溃后 `ClaimPendingTasks` 恢复未完成任务，执行 `go test -race ./internal/store/...`。
- **Step 2 (`scheduler`)：** 用 v1.0 `mockllm` 测 10 并发 + `60 RPM` 限流 + 429 指数退避重试 + `Ctrl+C` 优雅停机保存状态，执行 `go test -race ./internal/scheduler/...`。
- **Step 3 (`api`)：** 跑通 `httptest` 接口测试与 SSE 进度流接收，执行 `go test -race ./internal/api/...`。
- **Step 4–6 (`runner` / `scorer` / `report`)：** 用 `configs/eval.example.json` 对 `testdata/humaneval_mini.jsonl` 与 `testdata/toolcall_mini.jsonl` 跑通完整命令行 `evalreel eval -config ...` 与网页界面启动 `evalreel serve`，核对生成出来的单个 `report.html` 在断网环境下能正常打开和筛选差异题目。

# EvalReel v1.0 详细设计文档

## 1. v1.0 目标、环境要求与目录结构

### 1.1 版本定位与边界
v1.0 只做一件事：**把面向大模型与 Agent（能自主调用工具的智能体）的本地透明流式代理、轨迹录制与混沌故障注入做扎实，并跑通四层自动化与真机测试。**

- **v1.0 包含的内容：** `mockllm`（模拟大模型服务）、`proxy`（透明反向代理与优雅停机）、`sse`（流式旁路解析器）、`recorder`（并发安全 `.jsonl.gz` 轨迹落盘）、`chaos`（请求前与流式中的故障注入）、YAML（纯文本配置格式）配置文件加载以及 `pprof`（Go 内置性能剖析端口）。
- **v1.0 明确不做的内容：** 不接入 MySQL、Redis、Asynq 任务队列，不写网页管理后台，不做 MCP（大模型工具协议）代理，不做第 $N$ 步批量跑分调度（这些模块在 v2.0 与 v3.0 分阶段加入）。
- **为什么 v1.0 暂不用 Gin 和 Gorm：** v1.0 只需要一个轻量级的透明反向代理入口和简单的本地文件落盘。直接使用 Go 标准库 `net/http` 和 `net/http/httputil.ReverseProxy` 可以把转发延迟和内存拷贝降到最低；Gin（Web 框架）和 Gorm（数据库操作库）会在 v2.0 引入本地任务调度与 SQLite 状态管理时再接入。

### 1.2 运行环境与依赖选型
- **Go 版本要求：Go 1.23 及以上。** 原因：`tmaxmax/go-sse` 库的 `sse.Read` 函数使用了 Go 1.23 新增的迭代器语法（`for ev, err := range sse.Read(...)`）。
- **核心第三方库（`go.mod`）：**
  1. `github.com/tmaxmax/go-sse`：严格遵守 HTML5 SSE（服务端持续单向推送数据的流式协议）规范的解析库，支持通过 `ReadConfig.MaxEventSize` 突破 64KB 单行长度限制。
  2. `github.com/pkoukk/tiktoken-go`：词元（Token）本地计算库，只在模型接口未返回 `usage`（词元统计）时用来估算兜底。
  3. `gopkg.in/yaml.v3`：读取代理与混沌故障注入配置文件。
  4. `go.uber.org/goleak`（仅测试依赖）：在单元测试结束时自动检查是否有后台 Goroutine（Go 语言轻量级并发协程）没有退出，防止协程泄漏。

### 1.3 仓库目录结构

```text
EvalReel/
├── cmd/
│   └── evalreel/
│       └── main.go                 # 命令行入口（支持 proxy / mockllm 子命令）
├── configs/
│   └── proxy.example.yaml          # 代理与混沌故障注入示例配置
├── internal/
│   ├── config/                     # YAML 配置读取与默认值校验
│   │   └── config.go
│   ├── model/                      # 轨迹记录、性能指标与故障规则结构体
│   │   └── trace.go
│   ├── mockllm/                    # Step 1: 高仿真模拟 OpenAI 接口服务
│   │   ├── server.go
│   │   └── server_test.go
│   ├── proxy/                      # Step 2: 纯透传流式反向代理与优雅停机
│   │   ├── proxy.go
│   │   └── proxy_test.go           # 第 2 层集成测试（含 -race 与 goleak）
│   ├── sse/                        # Step 3: 旁路流式解析（Tap + go-sse）
│   │   ├── tap.go
│   │   ├── parser.go
│   │   └── parser_test.go          # 第 1 层脏数据表格驱动测试
│   ├── recorder/                   # Step 4: 并发安全 .jsonl.gz 落盘器
│   │   ├── recorder.go
│   │   └── recorder_test.go
│   └── chaos/                      # Step 5: 混沌故障注入（请求前 + 流式中）
│       ├── middleware.go
│       ├── stream.go
│       └── chaos_test.go           # 第 1 层故障规则单元测试
├── scripts/
│   ├── smoke_openai.py             # 第 3 层：15 行官方 Python SDK 冒烟脚本
│   └── loadtest.go                 # 第 4 层：50–100 并发稳定性与 pprof 压测脚本
├── docs/
└── go.mod
```

---

## 2. 核心数据流与多协议兼容

一次请求经过 EvalReel 代理时的完整生命周期如下：

```text
[客户端: Python SDK / curl / OpenGIS-Agent]
       │
       ▼  ① 发起请求（如 POST /v1/chat/completions 或 GET /v1/models）
┌──────────────────────────────────────────────────────────┐
│  Step 5: 请求前混沌中间件 (chaos.PreForwardMiddleware)    │
│  • 匹配规则 -> 触发固定延迟 / 429(带Retry-After) / 500   │
│  • 若提前拦截，直接构造 TraceRecord 落盘并返回给客户端    │
└──────────────────────────────┬───────────────────────────┘
                               │ ② 放行，读取并还原 r.Body（上限 10MB）
                               ▼
┌──────────────────────────────────────────────────────────┐
│  Step 2: 流式反向代理 (httputil.ReverseProxy)             │
│  • FlushInterval: -1（收到一块立刻刷给客户端，不攒批）    │
│  • 删除 Accept-Encoding + Transport.DisableCompression   │
└──────────────────────────────┬───────────────────────────┘
                               │ ③ 转发给真实上游或 mockllm
                               ▼
                   [上游: mockllm / DeepSeek / OpenAI]
                               │ ④ 返回响应头与响应体流
                               ▼
┌──────────────────────────────────────────────────────────┐
│  ModifyResponse 钩子（按响应 Content-Type 分流处理）       │
│  • 场景 A (text/event-stream 流式):                      │
│      resp.Body -> [流中混沌包装器] -> [Step 3: SSE Tap]   │
│  • 场景 B (application/json 非流式对话):                  │
│      读取完整 JSON 解析内容与 usage，还原 Body 后返回      │
│  • 场景 C (其他路径如 GET /v1/models):                    │
│      原样透传，不解析也不落盘                              │
└──────────────┬───────────────────────────┬───────────────┘
               │ 零延迟透传                │ 流结束或客户端中途断连触发 Close()
               ▼                           ▼
         [客户端实时接收]          [Step 4: Recorder 加锁写入 .jsonl.gz]
```

**为什么要按三种响应类型分流处理：**
真实 Agent 框架在启动或跑多轮任务时，除了发起流式对话（`stream: true`），有时还会请求 `GET /v1/models` 检查可用模型，或用非流式（`stream: false`）请求来提取结构化总结。如果代理假设所有响应都是 SSE 流，非流式对话的轨迹就会丢失，甚至会导致客户端初始化报错。

---

## 3. 核心数据结构与配置文件设计

### 3.1 轨迹记录结构（`internal/model/trace.go`）

每完成一次对话调用（包含正常结束、中途取消或被混沌故障拦截），`recorder` 都会向 `.jsonl.gz` 文件写入一行 JSON 记录。字段命名参考 OpenTelemetry GenAI（云原生可观测性标准）规范，方便后续版本平滑对接看板或分析工具。

```go
package model

import (
	"encoding/json"
	"time"
)

type TraceStatus string

const (
	StatusOK            TraceStatus = "ok"             // 正常结束
	StatusAborted       TraceStatus = "aborted"        // 客户端中途主动断连取消
	StatusError         TraceStatus = "error"          // 上游返回非 200 或流中途报错
	StatusChaosInjected TraceStatus = "chaos_injected" // 被混沌中间件主动注入故障
)

type ToolCall struct {
	Index     int    `json:"index"`
	ID        string `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // 多片流式 JSON 拼接后的完整参数字符串
}

type TraceMetrics struct {
	TTFTReasoningMs int64   `json:"ttft_reasoning_ms"` // 从发出请求到收到首个思考字的时间（毫秒）
	TTFTAnswerMs    int64   `json:"ttft_answer_ms"`    // 从发出请求到收到首个正文回答字的时间（毫秒）
	TotalMs         int64   `json:"total_ms"`          // 整个请求总耗时（毫秒）
	TPS             float64 `json:"tps"`               // 每秒生成词元数（按总输出 Token / 生成耗时估算）
	InputTokens     int     `json:"input_tokens"`
	OutputTokens    int     `json:"output_tokens"`
	TokensEstimated bool    `json:"tokens_estimated"`  // 为 true 表示上游未返回 usage，由 tiktoken-go 估算
}

type ChaosInfo struct {
	RuleName  string `json:"rule_name"`
	FaultType string `json:"fault_type"`
}

type TraceRecord struct {
	TraceID       string          `json:"trace_id"`
	SessionID     string          `json:"session_id"`
	Timestamp     time.Time       `json:"timestamp"`
	Method        string          `json:"method"`
	Path          string          `json:"path"`
	StatusCode    int             `json:"status_code"`
	Status        TraceStatus     `json:"status"`
	RequestModel  string          `json:"gen_ai.request.model,omitempty"`
	ResponseModel string          `json:"gen_ai.response.model,omitempty"`
	RequestBody   json.RawMessage `json:"request_body,omitempty"`  // 保留原始 messages 与 tools 字段
	ReasoningText string          `json:"reasoning_content,omitempty"`
	ContentText   string          `json:"content,omitempty"`
	ToolCalls     []ToolCall      `json:"tool_calls,omitempty"`
	FinishReason  string          `json:"finish_reason,omitempty"`
	RawError      string          `json:"raw_error,omitempty"`
	Metrics       TraceMetrics    `json:"metrics"`
	Chaos         *ChaosInfo      `json:"chaos,omitempty"`
}
```

### 3.2 配置文件规范（`configs/proxy.example.yaml`）

```yaml
server:
  listen: "127.0.0.1:8080"
  pprof_listen: "127.0.0.1:6060" # 性能分析端口，仅本地监听；留空则不开启
  shutdown_timeout: "15s"        # 收到 Ctrl+C 后等待在途请求落盘的最长时间

upstream:
  base_url: "https://api.deepseek.com" # 测试时可改为 mockllm 地址 http://127.0.0.1:9090
  timeout: "120s"                      # 单次请求上游的最大超时时间

recorder:
  output_dir: "./traces"         # 轨迹文件输出目录，自动生成 session_<时间戳>.jsonl.gz
  session_id: ""                 # 留空时自动生成
  max_request_body_mb: 10        # 防止超大请求把代理内存撑爆

chaos:
  enabled: false
  rules:
    - name: "every_2nd_rate_limit"
      match_path: "/v1/chat/completions"
      every_n: 2                 # 每第 2 次请求触发一次（0 表示不按固定间隔触发）
      probability: 0.0           # 触发概率 0.0~1.0（与 every_n 二选一，优先判断 every_n）
      action: "status_429"       # 可选: delay | status_429 | status_500 | cut_stream | malformed_json
      retry_after_sec: 2
    - name: "cut_after_5_chunks"
      match_path: "/v1/chat/completions"
      every_n: 0
      probability: 0.0
      action: "cut_stream"
      cut_after_chunks: 5        # 流式吐出 5 个有效 data 事件后强行切断连接
```

---

## 4. 五步模块化详细设计（含避坑要点）

### Step 1：`internal/mockllm`（高仿真模拟 OpenAI 服务）

**职责：** 提供一个可控的本地靶场服务器，让测试脚本不需要消耗真实 API 余额就能测出各类正常流式、思考模型和异常边界场景。

- **暴露接口：**
  - `GET /v1/models`：返回固定的模拟模型列表 JSON。
  - `POST /v1/chat/completions`：解析请求体中的 `stream`、`stream_options.include_usage` 和自定义控制字段，或读取特定调试请求头（例如 `X-Mock-TTFT-Ms`、`X-Mock-Fault`、`X-Mock-Chunks`），按配置吐出响应。
- **核心模拟能力：**
  1. **首字等待（TTFT）：** 默认等待 `300ms`。**关键写法：** 不能直接调用 `time.Sleep(d)`，必须写成：
     ```go
     select {
     case <-time.After(ttft):
     case <-r.Context().Done():
         return // 客户端或代理断连时立刻退出，不再继续等待或生成
     }
     ```
     **原因：** 如果用 `time.Sleep`，当客户端在等待首字期间取消请求时，服务端协程会傻等到休眠结束，无法通过“100 毫秒内响应级联取消”的集成测试。
  2. **完整流式协议仿真：**
     - 按配置先发送若干条带 `delta.reasoning_content` 的 SSE 块，再发送带 `delta.content` 的 SSE 块；
     - 支持按配置拆分成多段 `delta.tool_calls`（首块带 `id`、`type`、`function.name`，后续 4 块每次只吐一小段 `function.arguments` 字符串）；
     - 若请求开启了 `stream_options.include_usage: true`，在最后发送一个 `choices: []` 且带有 `usage` 对象的 SSE 块，最后发送 `data: [DONE]\n\n`；
     - 每次写入响应后必须立刻调用 `w.(http.Flusher).Flush()`。
  3. **内置异常模式：** 通过配置或请求头触发 `429`（带 `Retry-After` 响应头）、`500`、吐出指定块数后直接强制关闭底层 TCP 连接（触发流式半途断连）、向 HTTP 200 的正常流中插入 `data: {"error":{"message":"upstream overloaded"}}`、以及发出超过 64KB 的超长单行 `data:`。

### Step 2：`internal/proxy`（纯透传流式反向代理）

**职责：** 接收客户端 HTTP 请求，安全地把请求和响应无延迟地转发过去，同时在请求离开前抓一份请求体、在响应回来时挂上旁路监控。

- **关键工程实现与避坑点：**
  1. **开启零缓冲实时刷新：** 创建 `httputil.ReverseProxy` 时必须显式设置 `FlushInterval: -1`。
     - **原因：** 默认情况下 Go 的反向代理会等待缓冲区攒够一定字节或每隔一段时间才刷新一次，会把原本逐字输出的流式体验变成一卡一卡的整段输出；设为 `-1` 表示每收到上游一小块数据就立刻刷出。
  2. **彻底禁止上游返回 gzip 压缩流：** 需要同时做两处设置——在 `Rewrite`（或 `Director`）里执行 `req.Header.Del("Accept-Encoding")`，**并且**在自定义的 `http.Transport` 中设置 `DisableCompression: true`。
     - **原因：** 仅删掉请求头还不够，Go 标准库的 `http.Transport` 在 `DisableCompression` 为 `false`（默认值）且未设置 `Accept-Encoding` 时，会自动偷偷往请求里补上 `Accept-Encoding: gzip` 并替上层解压。如果只靠删请求头而上游受某些中转配置影响仍返回压缩字节，或者需要精确统计原始流式报文，`DisableCompression: true` 能彻底保证拿到明文 SSE 流。
  3. **安全读取并还原请求体（`r.Body`）：** `r.Body` 是一个只能顺序读一遍的数据流。在进入 `ReverseProxy` 前，用 `io.ReadAll(io.LimitReader(r.Body, maxBytes))` 读出请求 JSON（提取模型名、`stream` 标记和 `messages`），放入请求 `context` 供后续录制使用，然后用 `r.Body = io.NopCloser(bytes.NewReader(bodyBytes))` 重新塞回去。
  4. **优雅处理客户端主动断连：** 自定义 `proxy.ErrorHandler`。当客户端主动取消请求导致 `errors.Is(err, context.Canceled)` 时，不再向客户端写 502 错误页，也不当作服务崩溃处理；由录制回调函数记录该次请求状态为 `aborted`。
  5. **连接池参数与优雅停机：**
     - 为 `http.Transport` 配置合理的 `MaxIdleConns`（如 200）、`MaxIdleConnsPerHost`（如 100）和 `IdleConnTimeout`（90 秒），避免高并发下反复建立 TCP 连接耗尽本地端口。
     - 在 `main.go` 中使用 `signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)` 监听退出信号；收到信号后调用 `srv.Shutdown(shutdownCtx)` 停止接收新连接并等待正在进行的流式响应结束，最后调用 `recorder.Close()` 把压缩文件尾部校验数据写完整。

### Step 3：`internal/sse`（旁路流式解析器 Tap）

**职责：** 在 `ReverseProxy.ModifyResponse` 钩子中给响应体 `resp.Body` 套上一层自定义的 `TapReadCloser`（实现了标准 `io.ReadCloser` 接口），让字节流在穿过代理发往客户端的同时，被 `tmaxmax/go-sse` 同步解析。

- **核心结构与防协程泄漏设计：**
  `TapReadCloser` 内部包含：原始 `orig io.ReadCloser`、管道写端 `pw *io.PipeWriter`、管道读端 `pr *io.PipeReader`、结束信号通道 `doneCh chan struct{}`、`sync.Once` 保证 `Close()` 只执行一次。

  具体流转步骤：
  1. **零延迟读转发（`Read(p []byte)`）：** 每次 `ReverseProxy` 调用 `Read(p)` 时，先从 `orig.Read(p)` 读出 `n` 字节；如果 `n > 0`，在返回之前同步检查并打点记录“收到首个非空原始数据的时间”，同时把 `p[:n]` 写入 `pw`（若 `pw` 已因为解析结束或异常关闭则忽略写入错误），随后立即把 `(n, err)` 返回给 `ReverseProxy`。
  2. **后台协程运行 `go-sse` 解析（关键避坑配置）：**
     ```go
     cfg := &sse.ReadConfig{
         MaxEventSize: 4 * 1024 * 1024, // 必须显式设为 4MB，go-sse 默认限制也是 64KB
     }
     ```
     **为什么要显式设置 `MaxEventSize`：** 查看 `tmaxmax/go-sse` 源码（`event.go`）可知，它的默认事件长度上限同样是 `64KB`；如果不传 `ReadConfig` 或不调大 `MaxEventSize`，一旦上游吐出一条超过 64KB 的超长 `data:` 行，`sse.Read` 一样会报错中止。
  3. **防死锁与零协程泄漏保证：** 后台解析协程在函数最开头写上：
     ```go
     defer func() {
         _, _ = io.Copy(io.Discard, pr) // 把管道里剩余未读字节彻底排空，防止 pw.Write 卡死
         _ = pr.Close()
         close(doneCh)                  // 通知 Close() 本协程已安全退出
     }()
     ```
     当 `ReverseProxy` 结束读取（无论是流正常到达 `EOF` 文件末尾，还是客户端断连触发 `Close()`）时，`TapReadCloser.Close()` 会依次：关闭 `orig` → 关闭 `pw`（触发管道读端收到 `EOF`）→ 等待 `<-doneCh` 确认解析协程完全退出 → 汇总最终指标并写入 `Recorder`。**这个关闭顺序保证了无论出现什么异常，解析协程绝对不会残留泄漏，而且 `onComplete` 回调触发时所有数据一定已经解析完毕。**

- **脏数据与思考模型解析规则：**
  1. **双 TTFT 拆分：** 针对每一个解析出的 JSON chunk，检查 `choices[0].delta.reasoning_content` 是否非空，若是且首次出现则记录 `TTFTReasoningMs` 并累加进 `ReasoningText`；检查 `choices[0].delta.content` 是否非空，若是且首次出现则记录 `TTFTAnswerMs` 并累加进 `ContentText`。
  2. **流式工具调用片段拼接：** 维护一个 `map[int]*model.ToolCall`，按每个 `delta.tool_calls` 元素里的 `index` 分别累加 `id`、`function.name` 和 `function.arguments` 字符串片段，流结束后按 `index` 从小到大排序输出切片。
  3. **缺失 `[DONE]`、缺失 `usage` 与流中错误兼容：**
     - 遇到 `data: [DONE]` 正常结束迭代；如果上游没发 `[DONE]` 就直接关闭流，在 `EOF` 时同样按正常解析结束处理，不视为失败。
     - 若整个流结束都没有收到 `usage` 字段，把 `TokensEstimated` 设为 `true`，调用 `tiktoken-go` 对请求 `messages` 和输出 `ReasoningText + ContentText + ToolCalls` 估算词元数。
     - 若 HTTP 状态码是 200，但某条 `data:` 解析出的 JSON 顶层包含 `"error"` 字段，记录 `RawError` 并把该条轨迹状态标为 `error`。

### Step 4：`internal/recorder`（并发安全 `.jsonl.gz` 落盘器）

**职责：** 把组装好的 `TraceRecord` 并发安全地压缩写入磁盘文件，同时用接口抽象为 v2.0 接入 SQLite/MySQL 留出位置。

- **接口定义：**
  ```go
  type Recorder interface {
      Write(rec model.TraceRecord) error
      Close() error
  }
  ```
- **v1.0 实现（`GzipJSONLRecorder`）的避坑点：**
  1. **多协程并发写入防损坏：** 在 `GzipJSONLRecorder` 内部持有一把互斥锁 `mu sync.Mutex`、`file *os.File`、`gz *gzip.Writer` 和 `closed bool`。每次 `Write(rec)` 必须先 `mu.Lock()` 再编码 JSON + `\n` 并写入 `gz`。如果不加锁，多个请求同时往同一个 `gzip.Writer` 写字节会导致压缩流错乱损坏、无法解压。
  2. **及时刷新防止丢数据：** 每次 `Write` 完调用 `gz.Flush()`，把内存压缩缓冲区的内容推送到底层文件句柄（避免程序跑了半小时硬盘里文件还是 0 字节）。
  3. **必须正确关闭写 gzip 文件尾：** `gzip` 格式文件末尾有一个 8 字节的校验和与原长度尾部信息。程序退出时必须调用 `recorder.Close()`（先 `gz.Close()` 再 `file.Close()`），否则用标准工具解压会报 `unexpected end of file`。同时在 `Write` 和 `Close` 中都要判断 `closed` 标志，防止优雅停机过程中既在关闭又被迟到请求写入引发异常。

### Step 5：`internal/chaos`（混沌故障注入中间件）

**职责：** 按 YAML 配置在两个精确位置注入故障，帮助开发者测试 Agent 在面对异常模型服务时的表现。

- **位置 1：请求前中间件（包裹整个 `ReverseProxy`）：**
  - 按配置中的 `match_path` 匹配路径；
  - 支持两种触发计数方式：`every_n > 0` 时使用无锁原子计数器 `atomic.Int64`（`count.Add(1) % every_n == 0`），保证并发下精确每隔 $N$ 次触发一次；否则按 `probability` 随机触发；
  - **支持动作：**
    - `delay`：使用 `select { case <-time.After(d): case <-r.Context().Done(): return }` 休眠，休眠完成后继续转发给上游；
    - `status_429`：设置响应头 `Content-Type: application/json` 和 `Retry-After: <N>`，返回状态码 `429` 与 OpenAI 标准格式的限流 JSON，写入一条 `status: "chaos_injected"` 的轨迹后直接返回，不消耗上游额度；
    - `status_500`：返回状态码 `500` 与服务端内部错误 JSON 并记录轨迹。
- **位置 2：流式中包装器（挂载在 `ModifyResponse` 内，套在原始 `resp.Body` 外层、位于 `TapReadCloser` 内部或外侧）：**
  - **推荐顺序：** `原始 resp.Body` → `流中混沌包装器 (chaosStreamReader)` → `SSE TapReadCloser` → `ReverseProxy`。
  - **为什么这样排顺序：** 把 `chaosStreamReader` 放在 `TapReadCloser` 上游，可以确保 `TapReadCloser` 录下来的数据与客户端实际收到的残缺/错误流完全一致，方便事后排查 Agent 是在收到哪半截数据时崩溃的。
  - **支持动作：**
    - `cut_stream`：边读取边扫描 `\n\n`（SSE 事件分隔边界），统计完整事件数；吐出 `cut_after_chunks`（例如 5）个事件后，下一次 `Read()` 直接返回自定义错误（`errors.New("chaos: forced stream cut")`），迫使 `ReverseProxy` 中断当前 HTTP 流响应；
    - `malformed_json`：吐出指定块数后，替换注入一段残缺的 JSON 事件 `data: {"choices":[{"delta":{"content":` 后结束，测试客户端解析容错。

---

## 5. 四层测试与验收方案

### 第一层：表格驱动单元测试（专打脏数据与规则边界）
覆盖文件：`internal/sse/parser_test.go`、`internal/chaos/chaos_test.go`。使用 Go 标准的表格驱动测试（Table-Driven Tests，即用切片定义多组输入和期望输出，在循环里统一执行断言）。

| 测试分组 | 构造的异常输入 / 场景 | 必须通过的断言 |
| --- | --- | --- |
| **超长单行事件** | 构造一条 `data:` 长度达到 `128KB`（大于 64KB）的 SSE 响应流 | 正常解析出完整 JSON 内容，不报 `token too long` 错误 |
| **思考模型隔离** | 先发 10 块 `reasoning_content`，间隔 `50ms` 后再发 5 块 `content` | `ReasoningText` 与 `ContentText` 完全隔离不混杂；`TTFTAnswerMs >= TTFTReasoningMs + 50` |
| **残缺结尾与缺字段** | 末尾没有 `data: [DONE]` 且全程无 `usage` 字段 | 正常收尾不报错；`TokensEstimated == true` 且 `InputTokens > 0`、`OutputTokens > 0` |
| **HTTP 200 流中带错** | 状态码 200，前 2 块正常输出，第 3 块塞入 `data: {"error":{"message":"rate limit"}}` | `RawError` 捕获错误信息，`Status == "error"` |
| **多片工具参数拼接** | 同一个 `tool_calls[0]` 拆成 5 个片段，每次吐一段 JSON 参数字符串 | 最终 `ToolCalls[0].Arguments` 拼回完整合法 JSON 字符串 |
| **混沌触发精确性** | 配置 `every_n: 2` 的 429 规则连发 6 次请求；另测 `cut_after_chunks: 5` | 仅第 2、4、6 次返回 HTTP 429 且带 `Retry-After` 头；`cut_stream` 精确在 5 块后报错断开 |

### 第二层：进程内高仿真集成测试 + 竞态与协程泄漏检查
覆盖文件：`internal/proxy/proxy_test.go`。使用 Go 标准库 `httptest.NewServer` 在同一个测试进程里同时启动 `mockllm` 和 `proxy`。

1. **TestMain 零协程泄漏门禁：**
   在 `internal/proxy/proxy_test.go` 和 `internal/sse/parser_test.go` 中加入：
   ```go
   func TestMain(m *testing.M) {
       goleak.VerifyTestMain(m)
   }
   ```
   **测试清理注意点：** `http.Client` 默认会开启长连接缓存（Keep-Alive），会在后台保留空闲连接读取协程。每个测试用例结束时必须调用 `client.CloseIdleConnections()` 以及 `proxyServer.Close()`、`mockServer.Close()`，确保只有真正泄露的业务协程才会触发 `goleak` 报错。
2. **中途断连级联取消测试：**
   - 启动带 `2s` 生成总耗时、每 `50ms` 吐 1 块的 `mockllm`，通过 `proxy` 发起流式请求；
   - 测试客户端收到第 3 个文本块后立刻调用 `cancel()` 取消上下文并退出读取；
   - **三项硬断言：**
     1. `mockllm` 在 `100ms` 内通过内部计数/通道信号感知到 `r.Context().Done()` 并立刻停止生成；
     2. `proxy` 不发生崩溃（panic），测试日志无非预期错误；
     3. 解压测试临时目录下的 `.jsonl.gz` 文件，能读到这条状态为 `aborted` 且保留了前 3 块已吐出内容的残缺轨迹。
3. **数据竞争强制扫描命令：**
   ```bash
   go test -race -count=3 ./...
   ```
   `-race` 会开启 Go 运行时竞态检测器，`-count=3` 连续跑三轮，专门暴露未加锁并发读写共享变量或 map 的隐患。

### 第三层：真实网络、真实模型与 OpenGIS-Agent 端到端冒烟测试

1. **15 行 Python 官方 SDK 校验（`scripts/smoke_openai.py`）：**
   使用官方 `openai` 库，配置 `base_url="http://127.0.0.1:8080/v1"`，开启 `stream=True` 和 `stream_options={"include_usage": True}`，分别指向一个带思考字段的廉价模型（如 DeepSeek 推理档位或轻量模型）和常见中转接口：
   - 终端实时打印每个词，核对无成段卡顿；
   - 运行后使用命令解压查看轨迹，核对 `reasoning_content`、`content` 与模型端返回的 `usage` 数值一致。
2. **`curl -N` 命令行极限流式与中断校验：**
   执行 `curl -N http://127.0.0.1:8080/v1/chat/completions ...`（`-N` 参数关闭 curl 自身缓冲），看到内容吐出到一半时按 `Ctrl+C` 强行断开；检查代理终端日志是否立刻输出上游取消信息，随后停止代理并解压确认轨迹已按 `aborted` 状态完整落盘。
3. **真实 OpenGIS-Agent 实战挂载：**
   将 `OpenGIS-Agent` 的环境变量 `OPENAI_BASE_URL` 指向 `http://127.0.0.1:8080/v1`，执行 2 道需要多步调用 GIS 工具的题目：
   - **第一遍（正常透传模式）：** 验证 Agent 全程运行无感，解压 `.jsonl.gz` 检查是否按时间顺序抓取到了每一轮的提示词、模型发出的工具调用参数以及后续回传给模型的工具执行结果；
   - **第二遍（混沌故障模式）：** 在配置里开启 `every_n: 2` 返回 `429` 和 `cut_after_chunks: 5` 断流，直接观察 OpenGIS-Agent 在面对限流和流式断连时是触发内部重试还是直接抛异常退出。

### 第四层：并发稳定性与内存回落压测

使用 `scripts/loadtest.go` 对完整链路进行 2 分钟持续压测，提前为技术文档和面试沉淀可验证的性能基线数据：

- **压测参数配置：**
  - `mockllm` 配置为首字延迟 `500ms` + 持续 `2s` 匀速输出 SSE；
  - `proxy` 开启 `20%` 概率的混沌故障注入（混合延迟、429 与 5 块断流）；
  - 压测脚本启动 `50` 到 `100` 个并发 Goroutine 持续循环发起请求，其中随机 `20%` 的请求在收到前几个字符后主动 `cancel()` 中断。
- **采样观测指标（通过 `http://127.0.0.1:6060/debug/pprof/` 采集）：**
  1. 压测开始前基线数据（`goroutine` 数量与 `HeapAlloc` 堆内存大小）；
  2. 压测进行到第 60 秒峰值期的数据；
  3. 压测结束并等待 10 秒后的数据。
- **通过标准：** 压测结束 10 秒后，Goroutine 数量回落到与压测前基线一致（允许 2 个以内的空闲 HTTP 连接误差），Heap 堆内存回落正常且在重复多轮压测中不出现持续单调上涨。

---

## 6. 建议实施顺序与每步自检清单

| 阶段 | 开发模块 | 完成后立刻执行的验收动作 |
| --- | --- | --- |
| **第 1 步** | `model` + `mockllm` | 启动 `mockllm`，用 `curl -N` 测通正常流、思考流和 429 头 |
| **第 2 步** | `recorder` | 写并发压写测试，`go test -race ./internal/recorder`，解压确认 `.jsonl.gz` 格式完整 |
| **第 3 步** | `sse` | 写完第 1 层表格驱动测试，跑通 4 类脏数据用例 |
| **第 4 步** | `proxy` + `config` + `main` | 挂上 `TapReadCloser` 和优雅停机，跑通第 2 层 `httptest` + `goleak` 取消测试 |
| **第 5 步** | `chaos` + 真实冒烟 + 压测 | 补齐故障注入单元测试，完成第 3 层 `OpenGIS-Agent` 实测和第 4 层 `pprof` 2 分钟压测记录数据 |

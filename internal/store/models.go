package store

import "time"

// RunStatus 表示整场评测总任务的当前状态。
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
)

// TaskStatus 表示单条子任务（一道题 × 一个模型 × 第 k 遍）的状态机流转。
type TaskStatus string

const (
	TaskPending TaskStatus = "pending" // 等待进入队列执行
	TaskRunning TaskStatus = "running" // 正在请求模型或运行 Agent
	TaskSuccess TaskStatus = "success" // 已完成并拿到有效结果
	TaskFailed  TaskStatus = "failed"  // 重试耗尽或发生不可恢复错误
)

// EvalRun 对应数据库表 eval_runs，记录一次评测总任务的全局信息与进度计数。
type EvalRun struct {
	ID             string     `gorm:"primaryKey;size:64" json:"id"`
	IdempotencyKey *string    `gorm:"size:128;uniqueIndex" json:"idempotency_key,omitempty"`
	Name           string     `gorm:"size:128" json:"name"`
	Mode           string     `gorm:"size:32;not null" json:"mode"`
	Status         RunStatus  `gorm:"size:24;not null;index" json:"status"`
	ConfigJSON     string     `gorm:"type:longtext;not null" json:"config_json"`
	TotalTasks     int        `gorm:"not null" json:"total_tasks"`
	DoneTasks      int        `gorm:"not null;default:0" json:"done_tasks"`
	FailedTasks    int        `gorm:"not null;default:0" json:"failed_tasks"`
	CreatedAt      time.Time  `gorm:"index:idx_runs_created_at" json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
}

// EvalTask 对应数据库表 eval_tasks，存储每条子任务的输入快照、输出与耗时指标。
type EvalTask struct {
	ID              string     `gorm:"primaryKey;size:96" json:"id"`
	RunID           string     `gorm:"size:64;not null;index:idx_run_status,priority:1" json:"run_id"`
	ItemID          string     `gorm:"size:64;not null;index" json:"item_id"`
	TargetName      string     `gorm:"size:64;not null" json:"target_name"`
	TrialIndex      int        `gorm:"not null" json:"trial_index"`
	Status          TaskStatus `gorm:"size:24;not null;index:idx_run_status,priority:2" json:"status"`
	RetryCount      int        `gorm:"not null;default:0" json:"retry_count"`
	TraceID         string     `gorm:"size:96" json:"trace_id"`
	RequestSnapshot string     `gorm:"type:longtext" json:"request_snapshot"`
	ReasoningText   string     `gorm:"type:longtext" json:"reasoning_text"`
	ResponseText    string     `gorm:"type:longtext" json:"response_text"`
	ToolCallsJSON   string     `gorm:"type:longtext" json:"tool_calls_json"`
	TTFTMs          int64      `gorm:"not null;default:0" json:"ttft_ms"`
	TotalMs         int64      `gorm:"not null;default:0" json:"total_ms"`
	InputTokens     int        `gorm:"not null;default:0" json:"input_tokens"`
	OutputTokens    int        `gorm:"not null;default:0" json:"output_tokens"`
	CostUSD         float64    `gorm:"not null;default:0" json:"cost_usd"`
	ErrorMsg        string     `gorm:"type:longtext" json:"error_msg"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// EvalScore 对应数据库表 eval_scores，存储每条子任务在各个批卷器下的得分明细。
type EvalScore struct {
	ID          uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID      string    `gorm:"size:96;not null;index" json:"task_id"`
	RunID       string    `gorm:"size:64;not null;index" json:"run_id"`
	ScorerName  string    `gorm:"size:64;not null" json:"scorer_name"`
	Passed      bool      `gorm:"not null" json:"passed"`
	RubricHash  string    `gorm:"size:64" json:"rubric_hash"`
	Reason      string    `gorm:"type:longtext" json:"reason"`
	DetailsJSON string    `gorm:"type:longtext" json:"details_json"`
	CreatedAt   time.Time `json:"created_at"`
}

// EvalReport 对应数据库表 eval_reports，缓存汇总统计指标与生成好的单文件 HTML。
type EvalReport struct {
	RunID       string    `gorm:"primaryKey;size:64" json:"run_id"`
	SummaryJSON string    `gorm:"type:longtext;not null" json:"summary_json"`
	HTMLContent string    `gorm:"type:longtext;not null" json:"html_content"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// TraceSpan 对应数据库表 trace_spans，记录多 Agent 嵌套调用链中的每个节点与耗时。
type TraceSpan struct {
	SpanID       string    `gorm:"primaryKey;size:32" json:"span_id"`
	ParentSpanID string    `gorm:"size:32;index" json:"parent_span_id,omitempty"`
	TraceID      string    `gorm:"size:64;not null;index:idx_spans_trace_id" json:"trace_id"`
	RunID        string    `gorm:"size:64;index:idx_spans_run_id" json:"run_id,omitempty"`
	TaskID       string    `gorm:"size:96;index:idx_spans_task_id" json:"task_id,omitempty"`
	AgentName    string    `gorm:"size:64" json:"agent_name,omitempty"`
	Operation    string    `gorm:"size:64" json:"operation,omitempty"`
	Model        string    `gorm:"size:64" json:"model,omitempty"`
	StartTimeMs  int64     `gorm:"not null;default:0" json:"start_time_ms"`
	DurationMs   int64     `gorm:"not null;default:0" json:"duration_ms"`
	TTFTMs       int64     `gorm:"not null;default:0" json:"ttft_ms"`
	InputTokens  int       `gorm:"not null;default:0" json:"input_tokens"`
	OutputTokens int       `gorm:"not null;default:0" json:"output_tokens"`
	Status       string    `gorm:"size:24;not null" json:"status"`
	ErrorMsg     string    `gorm:"type:longtext" json:"error_msg,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// TaskResultUpdate 汇总子任务执行完毕后需要回写到数据库的字段。
type TaskResultUpdate struct {
	Status        TaskStatus
	RetryCount    int
	TraceID       string
	ReasoningText string
	ResponseText  string
	ToolCallsJSON string
	TTFTMs        int64
	TotalMs       int64
	InputTokens   int
	OutputTokens  int
	CostUSD       float64
	ErrorMsg      string
}

//go:build ignore

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := flag.String("dsn", "evalreel:evalreel_dev@tcp(127.0.0.1:3306)/evalreel?charset=utf8mb4&parseTime=True&loc=UTC", "MySQL DSN")
	totalRows := flag.Int("rows", 300000, "模拟插入的 eval_tasks 总行数")
	totalRuns := flag.Int("runs", 500, "模拟插入的 eval_runs 总场数")
	keepTables := flag.Bool("keep", false, "结束后是否保留临时压测表 bench_*")
	flag.Parse()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping mysql: %v", err)
	}

	cleanup := func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS bench_eval_tasks")
		_, _ = db.Exec("DROP TABLE IF EXISTS bench_eval_runs")
	}
	cleanup()
	if !*keepTables {
		defer cleanup()
	}

	// 1. 先建“无二级索引”的基线测试表（只有主键）。
	mustExec(db, `CREATE TABLE bench_eval_runs (
		id VARCHAR(64) PRIMARY KEY,
		name VARCHAR(128),
		status VARCHAR(24) NOT NULL,
		total_tasks INT NOT NULL,
		done_tasks INT NOT NULL,
		created_at DATETIME(3) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	mustExec(db, `CREATE TABLE bench_eval_tasks (
		id VARCHAR(96) PRIMARY KEY,
		run_id VARCHAR(64) NOT NULL,
		item_id VARCHAR(64) NOT NULL,
		target_name VARCHAR(64) NOT NULL,
		trial_index INT NOT NULL,
		status VARCHAR(24) NOT NULL,
		total_ms BIGINT NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	// 2. 批量造数（500 场 run + 300,000 条 task）。
	fmt.Printf("开始造数: %d 场 eval_runs, %d 条 eval_tasks...\n", *totalRuns, *totalRows)
	startInsert := time.Now()
	insertRuns(db, *totalRuns)
	insertTasks(db, *totalRuns, *totalRows)
	mustExec(db, "ANALYZE TABLE bench_eval_runs, bench_eval_tasks")
	fmt.Printf("造数与统计完成，耗时: %s\n\n", time.Since(startInsert).Round(time.Millisecond))

	targetRun := fmt.Sprintf("bench_run_%04d", *totalRuns/2)
	queries := []struct {
		label string
		sql   string
	}{
		{
			label: "Q1 断点续跑查询 (run_id + status)",
			sql:   fmt.Sprintf("SELECT id, status FROM bench_eval_tasks WHERE run_id = '%s' AND status IN ('pending', 'running') ORDER BY trial_index ASC, id ASC", targetRun),
		},
		{
			label: "Q2 战报聚合查询 (run_id)",
			sql:   fmt.Sprintf("SELECT id, item_id, target_name, status, total_ms FROM bench_eval_tasks WHERE run_id = '%s' ORDER BY id ASC", targetRun),
		},
		{
			label: "Q3 历史列表分页 (ORDER BY created_at DESC LIMIT 20)",
			sql:   "SELECT id, name, status, created_at FROM bench_eval_runs ORDER BY created_at DESC LIMIT 20",
		},
	}

	fmt.Println("========== 阶段 A：未建二级索引（仅 PRIMARY KEY） ==========")
	for _, q := range queries {
		runExplain(db, q.label, q.sql)
	}

	// 3. 加上与 EvalReel v3.0 模型一致的二级索引。
	fmt.Println("========== 创建 v3.0 联合索引与排序索引 ==========")
	idxStart := time.Now()
	mustExec(db, "CREATE INDEX idx_bench_run_status ON bench_eval_tasks (run_id, status)")
	mustExec(db, "CREATE INDEX idx_bench_runs_created ON bench_eval_runs (created_at)")
	mustExec(db, "ANALYZE TABLE bench_eval_runs, bench_eval_tasks")
	fmt.Printf("索引创建完成，耗时: %s\n\n", time.Since(idxStart).Round(time.Millisecond))

	fmt.Println("========== 阶段 B：命中 v3.0 索引后 ==========")
	for _, q := range queries {
		runExplain(db, q.label, q.sql)
	}
}

func insertRuns(db *sql.DB, totalRuns int) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var sb strings.Builder
	sb.WriteString("INSERT INTO bench_eval_runs (id, name, status, total_tasks, done_tasks, created_at) VALUES ")
	for i := 0; i < totalRuns; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		ts := base.Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05.000")
		fmt.Fprintf(&sb, "('bench_run_%04d','run_%04d','running',600,300,'%s')", i, i, ts)
	}
	mustExec(db, sb.String())
}

func insertTasks(db *sql.DB, totalRuns, totalRows int) {
	const batchSize = 2000
	statuses := []string{"success", "success", "success", "failed", "pending", "running"}
	for start := 0; start < totalRows; start += batchSize {
		end := start + batchSize
		if end > totalRows {
			end = totalRows
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO bench_eval_tasks (id, run_id, item_id, target_name, trial_index, status, total_ms) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteByte(',')
			}
			runIdx := i % totalRuns
			st := statuses[i%len(statuses)]
			fmt.Fprintf(&sb, "('btk_%07d','bench_run_%04d','item_%03d','qwen3.8-flash',%d,'%s',%d)",
				i, runIdx, i%200, (i/200)%3, st, 300+(i%1500))
		}
		mustExec(db, sb.String())
	}
}

func runExplain(db *sql.DB, label, query string) {
	fmt.Printf("--- %s ---\n", label)

	// 1. 标准 EXPLAIN 看访问类型、命中的索引与预估扫描行数。
	rows, err := db.Query("EXPLAIN " + query)
	if err != nil {
		log.Fatalf("EXPLAIN failed: %v", err)
	}
	cols, _ := rows.Columns()
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		_ = rows.Scan(ptrs...)
		m := make(map[string]string, len(cols))
		for i, c := range cols {
			if vals[i] == nil {
				m[c] = "NULL"
			} else {
				m[c] = string(vals[i])
			}
		}
		fmt.Printf("EXPLAIN: type=%s key=%s rows=%s Extra=%s\n", m["type"], m["key"], m["rows"], m["Extra"])
	}
	_ = rows.Close()

	// 2. EXPLAIN ANALYZE 实测真实耗时与实际扫描行数。
	t0 := time.Now()
	aRows, err := db.Query("EXPLAIN ANALYZE " + query)
	if err != nil {
		log.Fatalf("EXPLAIN ANALYZE failed: %v", err)
	}
	for aRows.Next() {
		var tree string
		_ = aRows.Scan(&tree)
		fmt.Printf("EXPLAIN ANALYZE (%s):\n%s\n", time.Since(t0).Round(time.Microsecond), strings.TrimSpace(tree))
	}
	_ = aRows.Close()
	fmt.Println()
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("exec failed: %v\nsql: %s", err, query)
	}
}

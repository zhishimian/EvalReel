package store

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// SQLiteStore 是 Store 接口的本地零配置实现，底层使用纯 Go SQLite 驱动。
type SQLiteStore struct {
	*gormStore
}

// NewSQLiteStore 打开或创建 SQLite 数据库文件，开启 WAL 模式并自动建表。
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	if dbPath != ":memory:" {
		dir := filepath.Dir(dbPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create db dir %s: %w", dir, err)
			}
		}
	}

	dsn := dbPath
	if dbPath != ":memory:" {
		dsn = dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	}

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying sql.DB: %w", err)
	}
	// 限制本地单文件数据库只保留 1 个连接排队写入，彻底避免多协程并发写库时出现 SQLITE_BUSY 报错。
	sqlDB.SetMaxOpenConns(1)

	gs := &gormStore{db: db}
	if err := gs.autoMigrate(); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return &SQLiteStore{gormStore: gs}, nil
}

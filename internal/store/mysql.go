package store

import (
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MySQLStore 是 Store 接口的分布式服务端实现，支持多控制面与多 Worker 并发读写。
type MySQLStore struct {
	*gormStore
}

// NewMySQLStore 连接 MySQL 8 实例，配置连接池参数并自动完成表结构迁移。
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	if dsn == "" {
		return nil, fmt.Errorf("mysql dsn is empty")
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying mysql sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	gs := &gormStore{db: db}
	if err := gs.autoMigrate(); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return &MySQLStore{gormStore: gs}, nil
}

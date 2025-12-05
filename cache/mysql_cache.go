package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/magic-lib/go-plat-utils/conv"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	onlyOneCleanMap = cmap.New[sync.Once]()
)

type MySQLCacheConfig struct {
	DSN       string
	SqlDB     *sql.DB
	TableName string `json:"table_name"`
	Namespace string `json:"namespace"`
}

// mySQLCache 基于MySQL实现的缓存
type mySQLCache[V any] struct {
	dsn string
	db  *sql.DB
	// 缓存表名，可在初始化时指定
	tableName string
	namespace string
}

// NewMySQLCache 创建MySQL缓存实例
func NewMySQLCache[V any](cfg *MySQLCacheConfig) (CommCache[V], error) {
	if cfg.SqlDB == nil {
		if cfg.DSN != "" {
			sqlDB, err := sql.Open("mysql", cfg.DSN)
			if err != nil {
				return nil, fmt.Errorf("初始化数据库连接失败: %v", err)
			}
			cfg.SqlDB = sqlDB
		}
	}

	if cfg.SqlDB == nil || cfg.TableName == "" {
		return nil, errors.New("请检查配置参数")
	}

	// 确保表存在，不存在则创建
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			namespace VARCHAR(50) NOT NULL,
			cache_key VARCHAR(255) NOT NULL,
			cache_value JSON NOT NULL,
			expire_time DATETIME DEFAULT NULL,
			create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace,cache_key) USING BTREE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
	`, cfg.TableName)

	_, err := cfg.SqlDB.Exec(createTableSQL)
	if err != nil {
		return nil, fmt.Errorf("创建缓存表失败: %v", err)
	}

	mysqlCache := &mySQLCache[V]{
		db:        cfg.SqlDB,
		tableName: cfg.TableName,
		namespace: cfg.Namespace,
	}

	onlyKey := fmt.Sprintf("%s/%s", cfg.DSN, cfg.TableName)
	if !onlyOneCleanMap.Has(onlyKey) {
		onlyOneCleanMap.Set(onlyKey, sync.Once{})
	}
	if onlyOneCleanData, ok := onlyOneCleanMap.Get(onlyKey); ok {
		// 每个表只用执行一次即可
		onlyOneCleanData.Do(func() {
			mysqlCache.startCleanupJob(checkInterval)
		})
	}
	return mysqlCache, nil
}

// Get 从缓存中获取值
func (c *mySQLCache[V]) Get(ctx context.Context, key string) (V, error) {
	var (
		valueStr    string
		expireBytes []byte
	)
	if ctx == nil {
		ctx = context.Background()
	}

	// 查询时自动过滤已过期的键
	querySQL := fmt.Sprintf(`SELECT cache_value, expire_time FROM %s WHERE namespace=? AND cache_key = ? AND (expire_time IS NULL OR expire_time > NOW()) LIMIT 1`, c.tableName)
	err := c.db.QueryRowContext(ctx, querySQL, c.namespace, key).Scan(&valueStr, &expireBytes)
	if err != nil {
		var zero V
		if errors.Is(err, sql.ErrNoRows) {
			// 键不存在或已过期，返回零值和错误
			return zero, nil
		}
		return zero, fmt.Errorf("查询缓存失败: %v", err)
	}
	// 反序列化JSON为指定类型
	return strToVal[V](valueStr)
}

// Set 向缓存中设置值，支持过期时间
func (c *mySQLCache[V]) Set(ctx context.Context, key string, val V, timeout time.Duration) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	valueStr := conv.String(val)
	if timeout == 0 {
		timeout = defaultMaxExpireTime
	}
	// 计算过期时间
	expireAt := time.Now().Add(timeout)

	// 插入或更新缓存（UPSERT操作）
	insertSQL := fmt.Sprintf(`INSERT INTO %s (namespace, cache_key, cache_value, expire_time) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE cache_value = VALUES(cache_value), expire_time = VALUES(expire_time), update_time = CURRENT_TIMESTAMP`, c.tableName)

	var args []interface{}
	args = append(args, c.namespace, key, valueStr, expireAt)

	result, err := c.db.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		return false, fmt.Errorf("设置缓存失败: %v", err)
	}

	// 检查是否影响了行
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}

// Del 从缓存中删除键
func (c *mySQLCache[V]) Del(ctx context.Context, key string) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE namespace=? AND cache_key = ?", c.tableName)
	result, err := c.db.ExecContext(ctx, deleteSQL, c.namespace, key)
	if err != nil {
		return false, fmt.Errorf("删除缓存失败: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	// 返回是否实际删除了数据
	return rowsAffected > 0, nil
}

// startCleanupJob 添加定时清理过期键的方法
func (c *mySQLCache[V]) startCleanupJob(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				cleanSQL := fmt.Sprintf("DELETE FROM %s WHERE expire_time IS NOT NULL AND expire_time < NOW()", c.tableName)
				_, err := c.db.Exec(cleanSQL)
				if err != nil {
					// 实际应用中建议使用日志库记录错误
					fmt.Printf("清理过期缓存失败: %v\n", err)
				}
			}
		}
	}()
}

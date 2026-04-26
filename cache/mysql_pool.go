package cache

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySqlPoolConfig MySql连接池配置
type MySqlPoolConfig struct {
	DSN             string        // 数据源名称
	MaxSize         int           // 最大连接数
	MaxUsage        time.Duration // 连接最大使用时间
	MaxOpenConns    int           // 最大打开连接数（sql.DB内部连接池）
	MaxIdleConns    int           // 最大空闲连接数（sql.DB内部连接池）
	ConnMaxLifetime time.Duration // 连接最大生命周期（sql.DB内部）
	ConnMaxIdleTime time.Duration // 连接最大空闲时间（sql.DB内部）
}

// MySqlPoolManager MySql连接池管理器
type MySqlPoolManager struct {
	manager *PoolManager[*sql.DB]
	configs map[string]*MySqlPoolConfig //每个mysql的配置
	mu      sync.RWMutex
}

const (
	defaultNamespace       = "default"
	defaultMaxOpenConns    = 20
	defaultMaxIdleConns    = 10
	defaultConnMaxLifetime = time.Hour
	defaultConnMaxIdleTime = 10 * time.Minute
)

var (
	defaultMySqlPoolManager *MySqlPoolManager
	onceMySqlManager        sync.Once
)

// DefaultMySqlPoolManager 获取默认的MySql连接池管理器
func DefaultMySqlPoolManager() *MySqlPoolManager {
	onceMySqlManager.Do(func() {
		defaultMySqlPoolManager = &MySqlPoolManager{
			manager: NewPoolManager[*sql.DB](),
			configs: make(map[string]*MySqlPoolConfig),
		}
	})
	return defaultMySqlPoolManager
}

// CreatePool 创建并注册一个MySql连接池
func (mpm *MySqlPoolManager) CreatePool(namespace, name string, config *MySqlPoolConfig) (*CommPool[*sql.DB], error) {
	if config.DSN == "" {
		return nil, fmt.Errorf("DSN不能为空")
	}

	key := getNsKey(namespace, name)

	mpm.mu.Lock()
	defer mpm.mu.Unlock()

	// 检查是否已存在
	if existingPool := mpm.manager.GetPool(namespace, name); existingPool != nil {
		return existingPool, nil
	}

	// 设置默认值
	if config.MaxOpenConns <= 0 {
		config.MaxOpenConns = defaultMaxOpenConns
	}
	if config.MaxIdleConns <= 0 {
		config.MaxIdleConns = defaultMaxIdleConns
	}
	if config.ConnMaxLifetime <= 0 {
		config.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if config.ConnMaxIdleTime <= 0 {
		config.ConnMaxIdleTime = defaultConnMaxIdleTime
	}

	// 创建资源池配置
	poolConfig := &ResPoolConfig[*sql.DB]{
		MaxSize:  config.MaxSize,
		MaxUsage: config.MaxUsage,
		New: func() (*sql.DB, error) {
			// 创建新的数据库连接
			db, err := sql.Open("mysql", config.DSN)
			if err != nil {
				return nil, fmt.Errorf("打开数据库连接失败: %v", err)
			}

			// 配置连接池参数
			db.SetMaxOpenConns(config.MaxOpenConns)
			db.SetMaxIdleConns(config.MaxIdleConns)
			db.SetConnMaxLifetime(config.ConnMaxLifetime)
			db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

			// 测试连接是否有效
			if err := db.Ping(); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("数据库连接测试失败: %v", err)
			}
			return db, nil
		},
		CheckFunc: func(db *sql.DB) error {
			// 检查连接是否有效
			return db.Ping()
		},
		CloseFunc: func(db *sql.DB) error {
			// 关闭数据库连接
			return db.Close()
		},
	}

	// 创建资源池
	pool := NewResPool(poolConfig)
	// 注册到管理器
	mpm.manager.SetPool(namespace, name, pool)
	// 保存配置
	mpm.configs[key] = config

	return pool, nil
}

// GetPool 获取已注册的MySql连接池
func (mpm *MySqlPoolManager) GetPool(namespace, name string) *CommPool[*sql.DB] {
	return mpm.manager.GetPool(namespace, name)
}

// GetDBResource 从指定连接池获取数据库连接
func (mpm *MySqlPoolManager) GetDBResource(namespace, name string) (Resource[*sql.DB], error) {
	pool := mpm.GetPool(namespace, name)
	if pool == nil {
		return nil, fmt.Errorf("连接池不存在: namespace=%s, name=%s", namespace, name)
	}

	resource, err := pool.Get()
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %v", err)
	}
	return resource, nil
}

// PutDBResource 从指定连接池放回数据库连接
func (mpm *MySqlPoolManager) PutDBResource(namespace, name string, res Resource[*sql.DB]) error {
	pool := mpm.GetPool(namespace, name)
	if pool == nil {
		return fmt.Errorf("连接池不存在: namespace=%s, name=%s", namespace, name)
	}
	pool.Put(res)
	return nil
}

// Exec 使用指定连接池执行数据库操作
func (mpm *MySqlPoolManager) Exec(namespace, name string, fn func(*sql.DB) error) error {
	pool := mpm.GetPool(namespace, name)
	if pool == nil {
		return fmt.Errorf("连接池不存在: namespace=%s, name=%s", namespace, name)
	}

	return pool.Exec(fn)
}

// ClosePool 关闭指定的连接池
func (mpm *MySqlPoolManager) ClosePool(namespace, name string) {
	pool := mpm.GetPool(namespace, name)
	if pool != nil {
		pool.Close()
		key := getNsKey(namespace, name)
		mpm.manager.pools.Remove(key)
	}
}

// CloseAll 关闭所有连接池
func (mpm *MySqlPoolManager) CloseAll() {
	mpm.mu.RLock()
	defer mpm.mu.RUnlock()

	for _, pool := range mpm.manager.GetAllPools() {
		if pool != nil {
			pool.Close()
		}
	}
}

// CreateMySqlPool 使用默认管理器创建MySql连接池
func CreateMySqlPool(config *MySqlPoolConfig) (*CommPool[*sql.DB], error) {
	return DefaultMySqlPoolManager().CreatePool(defaultNamespace, config.DSN, config)
}

// GetMySqlPool 使用默认管理器获取MySql连接池
func GetMySqlPool(dsn string) *CommPool[*sql.DB] {
	return DefaultMySqlPoolManager().GetPool(defaultNamespace, dsn)
}

// GetMySqlDB 使用默认管理器获取数据库连接
func GetMySqlDB(dsn string) (*sql.DB, error) {
	res, err := DefaultMySqlPoolManager().GetDBResource(defaultNamespace, dsn)
	if err != nil {
		return nil, err
	}
	return res.Get(), nil
}

// ExecWithMySqlPool 使用默认管理器执行数据库操作
func ExecWithMySqlPool(dsn string, fn func(*sql.DB) error) error {
	return DefaultMySqlPoolManager().Exec(defaultNamespace, dsn, fn)
}

// CloseMySqlPool 使用默认管理器关闭指定的连接池
func CloseMySqlPool(dsn string) {
	DefaultMySqlPoolManager().ClosePool(defaultNamespace, dsn)
}

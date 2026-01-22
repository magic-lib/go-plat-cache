package idempotent

import (
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"time"
)

// Config 幂等配置项
type Config struct {
	Namespace     string
	Cache         cache.CommCache[string]
	Expiration    time.Duration // 幂等过期时间
	ErrRepeat     error         // 重复请求的错误提示
	RollbackOnErr bool          // 业务执行失败时是否回滚缓存（删除Key，允许重试）
}

// 默认配置
var defaultConfig = Config{
	Namespace:     "idempotent",
	Cache:         cache.NewMemGoCache[string](5*time.Minute, 10*time.Minute),
	Expiration:    5 * time.Second,
	ErrRepeat:     fmt.Errorf("重复请求，请稍后再试"),
	RollbackOnErr: true,
}

// WithConfig 自定义配置
func WithConfig(cfg *Config) *Config {
	if cfg == nil {
		return &defaultConfig
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultConfig.Namespace
	}
	if cfg.Cache == nil {
		cfg.Cache = defaultConfig.Cache
	}
	if cfg.Expiration <= 0 {
		cfg.Expiration = defaultConfig.Expiration
	}
	if cfg.ErrRepeat == nil {
		cfg.ErrRepeat = defaultConfig.ErrRepeat
	}
	return cfg
}

// Do 执行幂等操作，核心原子逻辑
//func (c *Config) Do(ctx context.Context, key string, fn func() (any, error)) (any, error) {
//	// 1. 原子化写入缓存（NX逻辑，不存在则写入，存在则失败）
//	success, err := IdempotentCache.PutIfAbsent(ctx, cfg.Key, "LOCK", cfg.Expiration)
//	if err != nil {
//		return nil, fmt.Errorf("幂等缓存操作失败: %w", err)
//	}
//	if !success {
//		return nil, cfg.ErrRepeat
//	}
//
//	// 2. 执行业务方法
//	res, err := fn()
//
//	// 3. 业务执行失败，根据配置回滚缓存
//	if cfg.RollbackOnErr && err != nil {
//		_ = IdempotentCache.Delete(ctx, cfg.Key)
//	}
//
//	return res, err
//}

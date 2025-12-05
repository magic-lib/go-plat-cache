package cache

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

type redisCache[V any] struct {
	redisCfg *startupcfg.RedisConfig //redis配置
	rc       *redisClient
}

var (
	defaultRedisCfg *startupcfg.RedisConfig
)

// SetDefaultRedisConfig 切换默认的redis连接
func SetDefaultRedisConfig(con *startupcfg.RedisConfig) {
	if con != nil {
		defaultRedisCfg = con
	}
}

// getRealRedisConfig 获取真实的redis配置
func getRealRedisConfig(redisCfg ...*startupcfg.RedisConfig) *startupcfg.RedisConfig {
	if redisCfg == nil {
		redisCfg = make([]*startupcfg.RedisConfig, 0)
	}
	if defaultRedisCfg != nil {
		redisCfg = append(redisCfg, defaultRedisCfg)
	}

	for _, oneCfg := range redisCfg {
		if oneCfg == nil {
			continue
		}
		redisCli := NewRedisClient(oneCfg)
		connected := redisCli.CheckConnect()
		if connected {
			return oneCfg
		}
	}

	return nil
}

// NewRedisCache 新建
func NewRedisCache[V any](redisCfg ...*startupcfg.RedisConfig) (CommCache[V], error) {
	oneCfg := getRealRedisConfig(redisCfg...)
	if oneCfg != nil {
		return &redisCache[V]{
			redisCfg: oneCfg,
			rc:       NewRedisClient(oneCfg),
		}, nil
	}
	return nil, fmt.Errorf("redis NewRedisCache config error: %v", redisCfg)
}

// Get 从缓存中取得一个值
func (co *redisCache[V]) Get(ctx context.Context, key string) (V, error) {
	dataStr, err := co.rc.Get(getContext(ctx), key)
	if err != nil {
		var zero V
		return zero, err
	}
	return strToVal[V](dataStr)
}

// Set timeout为秒
func (co *redisCache[V]) Set(ctx context.Context, key string, val V, timeout time.Duration) (bool, error) {
	return co.rc.Set(getContext(ctx), key, conv.String(val), timeout)
}

// Del 从缓存中删除一个key
func (co *redisCache[V]) Del(ctx context.Context, key string) (bool, error) {
	return co.rc.Del(getContext(ctx), key)
}

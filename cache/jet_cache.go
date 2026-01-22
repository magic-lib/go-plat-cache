package cache

import (
	"context"
	"github.com/magic-lib/go-plat-utils/conv"
	jCache "github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/mgtv-tech/jetcache-go/remote"
	"time"

	"github.com/redis/go-redis/v9"
)

type JetCache[V any] struct {
	JetCacheGo jCache.Cache
}

type JetCacheConfig struct {
	FreeCacheSize       local.Size
	FreeCacheExpiration time.Duration
	Namespace           string
	RedisConfig         *redis.RingOptions
	RefreshDuration     time.Duration
	ErrNotFound         error
	JCacheOption        []jCache.Option
}

// NewJetCache 新建JetCache
func NewJetCache[V any](jConfig *JetCacheConfig) *JetCache[V] {
	if jConfig == nil {
		jConfig = &JetCacheConfig{}
	}
	if jConfig.Namespace == "" {
		jConfig.Namespace = "default"
	}

	jCacheOption := make([]jCache.Option, 0)
	if jConfig.RedisConfig != nil {
		ring := redis.NewRing(jConfig.RedisConfig)
		jCacheOption = append(jCacheOption, jCache.WithRemote(remote.NewGoRedisV9Adapter(ring)))
	}
	if jConfig.Namespace != "" {
		jCacheOption = append(jCacheOption, jCache.WithName(jConfig.Namespace))
	}
	if jConfig.Namespace != "" {
		jCacheOption = append(jCacheOption, jCache.WithName(jConfig.Namespace))
	}
	if jConfig.FreeCacheSize > 0 {
		if jConfig.FreeCacheExpiration <= 0 {
			jConfig.FreeCacheExpiration = time.Minute
		}
		jCacheOption = append(jCacheOption, jCache.WithLocal(local.NewFreeCache(jConfig.FreeCacheSize,
			jConfig.FreeCacheExpiration, jConfig.Namespace)))
	}
	if jConfig.RefreshDuration > 0 {
		jCacheOption = append(jCacheOption, jCache.WithRefreshDuration(jConfig.RefreshDuration))
	}
	if jConfig.ErrNotFound != nil {
		jCacheOption = append(jCacheOption, jCache.WithErrNotFound(jConfig.ErrNotFound))
	}
	if len(jConfig.JCacheOption) != 0 {
		jCacheOption = append(jCacheOption, jConfig.JCacheOption...)
	}
	jetCache := jCache.New(jCacheOption...)

	return &JetCache[V]{
		JetCacheGo: jetCache,
	}
}

// Get 从缓存中取得一个值
func (co *JetCache[V]) Get(ctx context.Context, key string) (v V, err error) {
	newV := conv.Pointer(v)
	err = co.JetCacheGo.Get(ctx, key, newV)
	if err != nil {
		return v, err
	}
	return conv.Convert[V](newV)
}

// Set timeout
func (co *JetCache[V]) Set(ctx context.Context, key string, val V, timeout time.Duration) (bool, error) {
	err := co.JetCacheGo.Set(ctx, key, jCache.Value(val), jCache.TTL(timeout))
	if err != nil {
		return false, err
	}
	return true, nil
}

// Del 从缓存中删除一个key
func (co *JetCache[V]) Del(ctx context.Context, key string) (bool, error) {
	err := co.JetCacheGo.Delete(ctx, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

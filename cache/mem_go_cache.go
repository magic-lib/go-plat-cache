package cache

import (
	"context"
	gCache "github.com/patrickmn/go-cache"
	"time"
)

type memGoCache[V any] struct {
	defaultExpiration, cleanupInterval time.Duration
	mCache                             *gCache.Cache
}

// NewMemGoCache 新建memGoCache
func NewMemGoCache[V any](defaultExpiration, cleanupInterval time.Duration) CommCache[V] {
	return &memGoCache[V]{
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
		mCache:            gCache.New(defaultExpiration, cleanupInterval),
	}
}

// Get 从缓存中取得一个值
func (co *memGoCache[V]) Get(ctx context.Context, key string) (v V, err error) {
	ret, ok := co.mCache.Get(key)
	if ok {
		if retVal, ok := ret.(V); ok {
			return retVal, nil
		}
	}
	return v, nil
}

// Set timeout为秒
func (co *memGoCache[V]) Set(_ context.Context, key string, val V, timeout time.Duration) (bool, error) {
	co.mCache.Set(key, val, timeout)
	return true, nil
}

// Del 从缓存中删除一个key
func (co *memGoCache[V]) Del(_ context.Context, key string) (bool, error) {
	co.mCache.Delete(key)
	return true, nil
}

package cache

import (
	"context"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"time"
)

type memLruCache[V any] struct {
	maxSize           int
	defaultExpiration time.Duration
	mCache            *expirable.LRU[string, V]
}

// NewMemLruCache 新建memGoCache
func NewMemLruCache[V any](maxSize int, expiration time.Duration) CommCache[V] {
	lruCacheClient := expirable.NewLRU[string, V](maxSize, nil, expiration)
	return &memLruCache[V]{
		maxSize:           maxSize,
		defaultExpiration: expiration,
		mCache:            lruCacheClient,
	}
}

// Get 从缓存中取得一个值
func (co *memLruCache[V]) Get(_ context.Context, key string) (v V, err error) {
	ret, ok := co.mCache.Get(key)
	if ok {
		return ret, nil
	}
	return v, nil
}

// Set timeout为秒
func (co *memLruCache[V]) Set(_ context.Context, key string, val V, _ time.Duration) (bool, error) {
	return co.mCache.Add(key, val), nil
}

// Del 从缓存中删除一个key
func (co *memLruCache[V]) Del(_ context.Context, key string) (bool, error) {
	return co.mCache.Remove(key), nil
}

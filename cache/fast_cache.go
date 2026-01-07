package cache

import (
	"context"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

type fastCache[V any] struct {
	mCache *fastcache.Cache
}

// NewFastCache 新建memGoCache
func NewFastCache[V any](maxSize int) CommCache[V] {
	if maxSize <= 1024 {
		maxSize = 128 * 1024 * 1024
	}
	return &fastCache[V]{
		mCache: fastcache.New(maxSize),
	}
}

// Get 从缓存中取得一个值
func (co *fastCache[V]) Get(_ context.Context, key string) (V, error) {
	var zero V
	data := co.mCache.Get(nil, []byte(key))
	if len(data) == 0 {
		return zero, nil
	}
	retString := string(data)
	return conv.Convert[V](retString)
}

// Set timeout为秒，由于fastcache不支持过期时间，这里只是简单设置值
func (co *fastCache[V]) Set(_ context.Context, key string, val V, _ time.Duration) (bool, error) {
	co.mCache.Set([]byte(key), []byte(conv.String(val)))
	return true, nil
}

// Del 从缓存中删除一个key
func (co *fastCache[V]) Del(_ context.Context, key string) (bool, error) {
	co.mCache.Del([]byte(key))
	return true, nil
}

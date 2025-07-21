package cache

import (
	"context"
	"github.com/gregjones/httpcache/diskcache"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

type diskCache[V any] struct {
	diskCache *diskcache.Cache
}

// NewDiskCache 新建diskCache
func NewDiskCache[V string](basePath string) *diskCache[string] {
	diskCacheInstance := diskcache.New(basePath)
	return &diskCache[string]{
		diskCache: diskCacheInstance,
	}
}

// Get 从缓存中取得一个值
func (co *diskCache[V]) Get(_ context.Context, key string) (v string, err error) {
	ret, ok := co.diskCache.Get(key)
	if ok {
		return string(ret), nil
	}
	return "", nil
}

// Set timeout无效
func (co *diskCache[V]) Set(_ context.Context, key string, val string, _ time.Duration) (bool, error) {
	co.diskCache.Set(key, []byte(conv.String(val)))
	return true, nil
}

// Del 从缓存中删除一个key
func (co *diskCache[V]) Del(_ context.Context, key string) (bool, error) {
	co.diskCache.Delete(key)
	return true, nil
}

package cache

import (
	"context"
	"github.com/gregjones/httpcache/diskcache"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

type DataWithExpiry struct {
	Data   string
	Expiry time.Time
}

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
		var dataWithExpiryRead DataWithExpiry
		err = conv.Unmarshal(ret, &dataWithExpiryRead)
		if err != nil {
			return "", err
		}
		// 判断是否过期
		if time.Now().After(dataWithExpiryRead.Expiry) {
			co.diskCache.Delete(key)
		}
		return dataWithExpiryRead.Data, nil
	}
	return "", nil
}

// Set timeout无效
func (co *diskCache[V]) Set(_ context.Context, key string, val string, timeout time.Duration) (bool, error) {
	dataWithExpiry := DataWithExpiry{
		Data:   val,
		Expiry: time.Now().Add(timeout),
	}
	serialized := conv.String(dataWithExpiry)
	co.diskCache.Set(key, []byte(serialized))
	return true, nil
}

// Del 从缓存中删除一个key
func (co *diskCache[V]) Del(_ context.Context, key string) (bool, error) {
	co.diskCache.Delete(key)
	return true, nil
}

package httpcache

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/magic-lib/go-plat-utils/goroutines"
	cmap "github.com/orcaman/concurrent-map"
	"time"
)

/*
 * 避免http并发量大时，造成后端数据库访问压力大，而缓慢，进行缓存读取
 * 每次如果命中以后，然后会执行 ExecuteGetDataHandle 更新缓存，这样可以达到实时更新的效果
 */

var (
	storeListCacheMap = cmap.New() //保存store的列表，避免重复创建
	saveDataTypeMap   = cmap.New() //保存每个对象的类型，避免错误初始化
)

func getStoreCacheKey(namespace string, cacheKey string) string {
	return fmt.Sprintf("{%s}%s", namespace, cacheKey)
}

// 同一个key的访问次数
func getLockCacheKey(namespace string, cacheKey string) string {
	return fmt.Sprintf("{%s}{lock-key}%s", namespace, cacheKey)
}

// 单个获取内容
func getDataFromCache[V any](ctx context.Context, storeList []cache.CommCache[*cacheData[V]], storeKey string) (value *cacheData[V], err error) {
	var lastErr error
	for _, oneFactory := range storeList {
		one, err := oneFactory.Get(ctx, storeKey)
		if err == nil && one != nil {
			return one, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	return value, lastErr
}

// 根据 store 取得数据
func multiGetData[V any](ctx context.Context, storeList []cache.CommCache[*cacheData[V]], namespace string, cacheKey string, timeout time.Duration) (value *cacheData[V], err error) {
	if storeList == nil || len(storeList) == 0 {
		return value, fmt.Errorf("multiGetData storeList empty")
	}
	storeKey := getStoreCacheKey(namespace, cacheKey)
	if timeout > 0 {
		value, err = goroutines.RunWithTimeout[*cacheData[V]](timeout, func() (*cacheData[V], error) {
			return getDataFromCache[V](ctx, storeList, storeKey)
		})
		return value, err
	}
	return getDataFromCache[V](ctx, storeList, storeKey)
}

// 根据 store 设置数据
func multiSetData[V any](ctx context.Context, storeList []cache.CommCache[*cacheData[V]], namespace string, cacheKey string, dataValue V, expiration time.Duration) (bool, error) {
	if storeList == nil || len(storeList) == 0 {
		return false, fmt.Errorf("multiSetData storeList empty")
	}

	if closed {
		return false, fmt.Errorf("closed")
	}
	storeKey := getStoreCacheKey(namespace, cacheKey)
	var lastErr error
	newCacheData := &cacheData[V]{
		data:           dataValue,
		createTime:     time.Now(),
		expirationTime: time.Now().Add(expiration),
	}
	for _, oneFactory := range storeList {
		_, err := oneFactory.Set(ctx, storeKey, newCacheData, expiration)
		if err == nil {
			return true, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	return false, lastErr
}

// 根据 store 删除数据
func multiDelData[V any](ctx context.Context, storeList []cache.CommCache[*cacheData[V]], namespace string, cacheKey string) (bool, error) {
	if storeList == nil || len(storeList) == 0 {
		return false, fmt.Errorf("multiSetData storeList empty")
	}

	storeKey := getStoreCacheKey(namespace, cacheKey)
	var lastErr error
	for _, oneFactory := range storeList {
		_, err := oneFactory.Del(ctx, storeKey)
		if err == nil {
			return true, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	return false, lastErr
}

package cache

import (
	cmap "github.com/orcaman/concurrent-map"
)

var (
	globalPool = cmap.New()
)

func GetGlobalPool(namespace, name string) any {
	key := getNsKey(namespace, name)
	if v, ok := globalPool.Get(key); ok {
		return v
	}
	return nil
}

func SetGlobalPool(namespace, name string, pool *CommPool[any]) {
	if pool == nil {
		return
	}
	key := getNsKey(namespace, name)
	if globalPool.Has(key) {
		if onePool, ok := globalPool.Get(key); ok {
			if one, ok := onePool.(*CommPool[any]); ok {
				one.Close() //关闭掉现在已存在的资源池
			}
		}
	}
	globalPool.Set(key, pool)
}

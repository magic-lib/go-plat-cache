package cache

import (
	cmap "github.com/orcaman/concurrent-map/v2"
)

// PoolManager 类型注册表，维护不同类型的池
type PoolManager[T any] struct {
	pools cmap.ConcurrentMap[string, *CommPool[T]]
}

// NewPoolManager 创建一个资源池管理器
func NewPoolManager[T any]() *PoolManager[T] {
	return &PoolManager[T]{
		pools: cmap.New[*CommPool[T]](),
	}
}

func (gpm *PoolManager[T]) SetPool(namespace, name string, pool *CommPool[T]) {
	if pool == nil {
		return
	}
	key := getNsKey(namespace, name)
	if gpm.pools.Has(key) {
		if onePool, ok := gpm.pools.Get(key); ok {
			onePool.Close() //关闭掉现在已存在的资源池
		}
	}
	gpm.pools.Set(key, pool)
}

func (gpm *PoolManager[T]) GetPool(namespace, name string) *CommPool[T] {
	key := getNsKey(namespace, name)
	if v, ok := gpm.pools.Get(key); ok {
		return v
	}
	return nil
}

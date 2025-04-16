package cache

import (
	"sync"
)

type objectPool[T any] struct {
	pool sync.Pool
}

// NewPoolObject 创建一个新的泛型 Pool 实例
func NewPoolObject[T any](newFunc func() T) *objectPool[T] {
	return &objectPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newFunc()
			},
		},
	}
}

// Get 从泛型 Pool 中获取一个对象
func (p *objectPool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put 将对象放回泛型 Pool 中
func (p *objectPool[T]) Put(x T) {
	p.pool.Put(x)
}

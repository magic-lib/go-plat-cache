package cache

import (
	"context"
	cuckoo "github.com/seiflotfy/cuckoofilter"
	"time"
)

var (
	defaultFilterSize = 1000000
)

type cuckooFilter[V bool] struct {
	cf *cuckoo.Filter
}

// NewCuckooFilter 创建过滤器实例
func NewCuckooFilter(capacity int) CommCache[bool] {
	if capacity <= 0 {
		capacity = defaultFilterSize
	}
	cf := cuckoo.NewFilter(uint(capacity))
	return &cuckooFilter[bool]{
		cf: cf,
	}
}

// Get 从缓存中获取值
func (c *cuckooFilter[V]) Get(_ context.Context, key string) (bool, error) {
	return c.cf.Lookup([]byte(key)), nil
}

// Set 向缓存中设置值
func (c *cuckooFilter[V]) Set(_ context.Context, key string, _ V, _ time.Duration) (bool, error) {
	return c.cf.InsertUnique([]byte(key)), nil
}

// Del 从缓存中删除键
func (c *cuckooFilter[V]) Del(_ context.Context, key string) (bool, error) {
	return c.cf.Delete([]byte(key)), nil
}

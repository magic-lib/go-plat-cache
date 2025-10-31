package cache

import "time"

// SetWithTTL 设置带过期时间的缓存条目。
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	item := &cacheItem{
		key:        key,
		value:      value,
		ttl:        ttl,
		expiration: time.Now().Add(ttl).UnixNano(),
	}
	sh := c.getShard(key)
	sh.set(item, weightOf(value))
	if c.persister != nil {
		c.enqueuePersist(key, value)
	}
}

// Get 获取 key 对应的缓存值，若过期则返回 nil。
func (c *Cache) Get(key string) (interface{}, bool) {
	sh := c.getShard(key)
	item, ok := sh.get(key)
	if !ok || item.isExpired() {
		return nil, false
	}
	return item.value, true
}

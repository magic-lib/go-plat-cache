package cache

import "time"

// cacheItem 表示缓存中的单个条目。
type cacheItem struct {
	key        string        // 缓存键
	value      interface{}   // 缓存值
	expiration int64         // 过期时间（UnixNano）
	ttl        time.Duration // 有效期
}

// isExpired 判断条目是否已过期。
func (it *cacheItem) isExpired() bool {
	if it.ttl <= 0 {
		return false // ttl<=0 表示永不过期
	}
	return time.Now().UnixNano() > it.expiration
}

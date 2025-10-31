package cache

// Persister 定义缓存持久化接口。
type Persister interface {
	Save(key string, value interface{}) error // 保存条目
	Load(key string) (interface{}, bool)      // 加载条目
}

// enqueuePersist 异步持久化缓存条目。
func (c *Cache) enqueuePersist(key string, value interface{}) {
	select {
	case c.backgroundPool <- struct{}{}:
		go func() {
			defer func() { <-c.backgroundPool }()
			if c.persister != nil {
				_ = c.persister.Save(key, value)
			}
		}()
	default:
		// 丢弃以避免阻塞
	}
}

package cache

import (
	"container/list"
	"context"
	"hash/fnv"
	"time"

	"golang.org/x/sync/singleflight"
)

// Weigher 用于自定义缓存条目的权重（如字节大小），影响 LRU 驱逐。
type Weigher func(key string, val interface{}) int64

// CacheOptions 用于配置缓存的行为和参数。
type CacheOptions struct {
	DefaultTTL     time.Duration // 默认过期时间
	EmptyTTL       time.Duration // loader 返回 nil 时的过期时间
	RefreshFactor  float64       // 异步刷新触发因子（剩余时间 < ttl*factor 时刷新）
	ShardCount     int           // 分片数量，影响并发性能
	TotalMaxBytes  int64         // 总最大缓存字节数
	BackgroundJobs int           // 后台持久化并发任务数
	Weigher        Weigher       // 权重计算函数
	Persister      Persister     // 持久化接口实现
}

// Cache 是主缓存结构，支持分片、LRU、TTL、singleflight、持久化等。
type Cache struct {
	shards         []*shard           // 分片数组
	group          singleflight.Group // singleflight 防止重复加载
	defaultTTL     time.Duration      // 默认 TTL
	emptyTTL       time.Duration      // 空值 TTL
	refreshFactor  float64            // 刷新因子
	backgroundPool chan struct{}      // 后台任务池
	weigher        Weigher            // 权重计算
	persister      Persister          // 持久化实现
	totalMaxBytes  int64              // 总最大字节数
}

// NewCache 创建一个新的 Cache 实例。
func NewCache(opt CacheOptions) *Cache {
	shards := make([]*shard, opt.ShardCount)
	for i := 0; i < opt.ShardCount; i++ {
		shards[i] = newShard()
		shards[i].capBytes = opt.TotalMaxBytes / int64(opt.ShardCount)
	}
	return &Cache{
		shards:         shards,
		defaultTTL:     opt.DefaultTTL,
		emptyTTL:       opt.EmptyTTL,
		refreshFactor:  opt.RefreshFactor,
		backgroundPool: make(chan struct{}, opt.BackgroundJobs),
		weigher:        opt.Weigher,
		persister:      opt.Persister,
		totalMaxBytes:  opt.TotalMaxBytes,
	}
}

// getShard 根据 key 哈希分配到对应分片。
func (c *Cache) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.shards[int(h.Sum32())%len(c.shards)]
}

// GetOrLoadCtx 先查缓存，miss 时用 loader 加载并写入，支持 singleflight 防抖。
func (c *Cache) GetOrLoadCtx(ctx context.Context, key string,
	loader func(context.Context) (interface{}, time.Duration, error)) (interface{}, error) {

	if val, ok := c.Get(key); ok {
		c.asyncRefresh(key, loader, val) // 命中时异步刷新
		return val, nil
	}

	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		val, ttl, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		if val == nil {
			ttl = c.emptyTTL
		}
		c.SetWithTTL(key, val, ttl)
		return val, nil
	})
	return v, err
}

// asyncRefresh 命中缓存时，剩余时间不足则异步刷新。
func (c *Cache) asyncRefresh(key string, loader func(context.Context) (interface{}, time.Duration, error), oldVal interface{}) {
	sh := c.getShard(key)
	item, ok := sh.items[key].Value.(*cacheItem)
	if !ok || item.ttl <= 0 {
		return
	}
	remain := time.Until(time.Unix(0, item.expiration))
	if remain < time.Duration(float64(item.ttl)*c.refreshFactor) {
		go func() {
			_, _, _ = c.group.Do(key, func() (interface{}, error) {
				val, ttl, err := loader(context.Background())
				if err == nil && val != nil {
					c.SetWithTTL(key, val, ttl)
				}
				return nil, nil
			})
		}()
	}
}

// Delete 删除指定 key 的缓存。
func (c *Cache) Delete(key string) {
	sh := c.getShard(key)
	sh.delete(key)
}

// Purge 清空所有分片缓存。
func (c *Cache) Purge() {
	for _, sh := range c.shards {
		sh.mu.Lock()
		sh.items = make(map[string]*list.Element)
		sh.lru.Init()
		sh.curBytes = 0
		sh.mu.Unlock()
	}
}

// ShardCount 返回分片数量。
func (c *Cache) ShardCount() int {
	return len(c.shards)
}

// ShardItems 返回指定分片的条目数量。
func (c *Cache) ShardItems(index int) int {
	if index < 0 || index >= len(c.shards) {
		return 0
	}
	c.shards[index].mu.Lock()
	defer c.shards[index].mu.Unlock()
	return len(c.shards[index].items)
}

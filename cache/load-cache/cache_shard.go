package cache

import (
	"container/list"
	"sync"
)

// shard 表示缓存的一个分片，包含 LRU、容量、条目等。
type shard struct {
	mu       sync.Mutex               // 分片锁
	items    map[string]*list.Element // key 到 LRU 节点的映射
	lru      *list.List               // LRU 链表
	capBytes int64                    // 分片最大容量（字节）
	curBytes int64                    // 当前已用容量
}

// newShard 创建一个新的分片。
func newShard() *shard {
	return &shard{
		items: make(map[string]*list.Element),
		lru:   list.New(),
	}
}

// get 获取 key 对应的条目，并将其移到 LRU 前端。
func (s *shard) get(key string) (*cacheItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ele, ok := s.items[key]; ok {
		s.lru.MoveToFront(ele)
		return ele.Value.(*cacheItem), true
	}
	return nil, false
}

// set 插入或更新条目，并根据权重调整容量，必要时驱逐。
func (s *shard) set(item *cacheItem, weight int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.items[item.key]; ok {
		s.lru.MoveToFront(ele)
		oldItem := ele.Value.(*cacheItem)
		s.curBytes -= weightOf(oldItem.value)
		ele.Value = item
		s.curBytes += weight
		return
	}
	// 新条目
	ele := s.lru.PushFront(item)
	s.items[item.key] = ele
	s.curBytes += weight
	s.evict()
}

// delete 删除指定 key 的条目。
func (s *shard) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ele, ok := s.items[key]; ok {
		item := ele.Value.(*cacheItem)
		s.curBytes -= weightOf(item.value)
		s.lru.Remove(ele)
		delete(s.items, key)
	}
}

// evict 按 LRU 驱逐，直到容量满足要求。
func (s *shard) evict() {
	for s.capBytes > 0 && s.curBytes > s.capBytes && s.lru.Len() > 0 {
		back := s.lru.Back()
		if back == nil {
			break
		}
		item := back.Value.(*cacheItem)
		s.curBytes -= weightOf(item.value)
		delete(s.items, item.key)
		s.lru.Remove(back)
	}
}

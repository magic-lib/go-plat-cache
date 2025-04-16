package cache

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/magic-lib/go-plat-utils/id-generator/id"
	"github.com/magic-lib/go-plat-utils/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"sync"
	"time"
)

const defaultMaxSize = 10
const defaultMaxUsage = 1 * time.Minute

// resInfo 用于记录资源及其获取时间
type resInfo[T any] struct {
	id         string    // 资源ID
	Resource   T         //具体资源
	usedTime   time.Time //使用时间
	createTime time.Time //创建时间
}

// ResPoolConfig 资源池结构体，传入参数
type ResPoolConfig[T any] struct {
	MaxSize   int               // 最大资源数量
	MaxUsage  time.Duration     // 资源最大使用时间
	New       func() (T, error) // 创建新资源的函数
	CheckFunc func(T) error     // 检查资源有效的函数
	CloseFunc func(T) error     // 关闭资源的函数
}

type CommPool[T any] struct {
	ResPoolConfig[T]

	idle       cmap.ConcurrentMap[string, *resInfo[T]] // 空闲资源列表
	used       cmap.ConcurrentMap[string, *resInfo[T]] // 已使用资源列表
	once       cmap.ConcurrentMap[string, *resInfo[T]] // 一次性资源列表
	delayClose cmap.ConcurrentMap[string, *resInfo[T]] // 延迟删除资源列表，避免将正在使用的删除掉了

	mu sync.RWMutex
}

// NewResPool 创建一个新的资源池
func NewResPool[T any](connPool *ResPoolConfig[T]) *CommPool[T] {
	pool := new(CommPool[T])
	pool.ResPoolConfig = *connPool
	if pool.MaxSize <= 0 {
		pool.MaxSize = defaultMaxSize
	}
	if pool.MaxUsage == 0 {
		pool.MaxUsage = defaultMaxUsage
	}
	if pool.New == nil {
		panic("new function is required")
	}
	pool.idle = cmap.New[*resInfo[T]]()
	pool.used = cmap.New[*resInfo[T]]()
	pool.once = cmap.New[*resInfo[T]]()
	pool.delayClose = cmap.New[*resInfo[T]]()

	// 默认放一个，用来检测是否可以创建
	resource, err := pool.create()
	if err != nil {
		fmt.Println("NewResPool error:", err.Error())
	} else {
		pool.idle.Set(resource.id, resource)
	}

	if pool.CheckFunc != nil {
		goroutines.GoAsync(func(params ...any) {
			pool.checkIdleResources()
		}, nil)
	}
	goroutines.GoAsync(func(params ...any) {
		pool.checkMaxUsageResources()
	}, nil)
	goroutines.GoAsync(func(params ...any) {
		pool.checkDelayCloseResources()
	}, nil)

	return pool
}

// Get 从资源池获取一个资源
func (p *CommPool[T]) create() (*resInfo[T], error) {
	//创建新的资源
	conn, err := p.New()
	if err != nil {
		return nil, err
	}
	id := id.NewUUID()
	if id == "" {
		id = utils.RandomString(10)
	}

	return &resInfo[T]{
		id:         id,
		Resource:   conn,
		createTime: time.Now(),
		usedTime:   time.Now(),
	}, nil
}

// Get 从资源池获取一个资源
func (p *CommPool[T]) Get() (*resInfo[T], error) {
	//如果存在空闲资源，直接返回
	if !p.idle.IsEmpty() {
		keyList := p.idle.Keys()
		var retResource *resInfo[T]
		lo.ForEachWhile(keyList, func(item string, index int) bool {
			if resource, ok := p.idle.Get(item); ok {
				resource.usedTime = time.Now()
				retResource = resource
				p.used.Set(resource.id, resource)
				p.idle.Remove(item)
				return false
			}
			return true
		})
		if retResource != nil {
			return retResource, nil
		}
	}

	resource, err := p.create()
	if err != nil {
		return nil, err
	}
	//已经满了，则直接创建，使用短连接
	if p.used.Count() == p.MaxSize {
		p.once.Set(resource.id, resource)
	} else {
		p.used.Set(resource.id, resource)
	}

	return resource, nil
}

// Put 将资源释放回资源池
func (p *CommPool[T]) Put(res *resInfo[T]) {
	if p.used.Has(res.id) {
		p.used.Remove(res.id)
		p.idle.Set(res.id, res)
		return
	}
	// 如果没有找到，可能是一次性资源，从一次性资源列表中移除
	if p.once.Has(res.id) {
		p.once.Remove(res.id)
		if p.idle.Count()+p.used.Count() < p.MaxSize {
			p.idle.Set(res.id, res)
			return
		}
	}
	if p.idle.Has(res.id) {
		return
	}
	// 超出最大资源数量，直接关闭
	_ = p.closeList(true, res)
}

// Exec 将资源释放回资源池
func (p *CommPool[T]) Exec(fun func(c T) error) error {
	res, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Put(res)
	return fun(res.Resource)
}

// Close 关闭资源池中的所有资源
func (p *CommPool[T]) Close() {
	p.idle.IterCb(func(key string, val *resInfo[T]) {
		_ = p.closeList(false, val)
	})
	p.used.IterCb(func(key string, val *resInfo[T]) {
		_ = p.closeList(false, val)
	})
	p.once.IterCb(func(key string, val *resInfo[T]) {
		_ = p.closeList(false, val)
	})
	p.idle.Clear()
	p.used.Clear()
	p.once.Clear()
}

// checkIdleResources 定期检查空闲资源的有效性
func (p *CommPool[T]) checkIdleResources() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			errList := make([]*resInfo[T], 0)
			p.idle.IterCb(func(key string, val *resInfo[T]) {
				err := p.CheckFunc(val.Resource)
				if err != nil {
					fmt.Printf("error checking idle resource: %v\n", err)
					errList = append(errList, val)
				}
			})

			//无失效情况，则跳过
			if len(errList) == 0 {
				continue
			}
			// 移除无效资源
			lo.ForEach(errList, func(item *resInfo[T], _ int) {
				// 尝试创建新资源
				resource, err := p.create()
				if err != nil {
					return
				}

				if p.idle.Has(item.id) {
					p.idle.Set(resource.id, resource)
					p.idle.Remove(item.id)
					_ = p.closeList(false, item)
				}
			})
		}
	}
}

// checkMaxUsageResources 检查超时使用的函数
func (p *CommPool[T]) checkMaxUsageResources() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			expiredResources := make([]*resInfo[T], 0)
			p.used.IterCb(func(key string, item *resInfo[T]) {
				if time.Since(item.usedTime) > p.MaxUsage {
					expiredResources = append(expiredResources, item)
				}
			})
			p.once.IterCb(func(key string, item *resInfo[T]) {
				if time.Since(item.usedTime) > p.MaxUsage {
					expiredResources = append(expiredResources, item)
				}
			})
			if len(expiredResources) == 0 {
				continue
			}
			//可能忘记调用Put方法，需要放入空闲列表中
			lo.ForEach(expiredResources, func(item *resInfo[T], i int) {
				if item != nil {
					p.Put(item)
				}
			})
		}
	}
}

// checkDelayCloseResources 检查延迟删除使用的函数,避免正在使用时，突然就关闭了
func (p *CommPool[T]) checkDelayCloseResources() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.delayClose.IterCb(func(key string, item *resInfo[T]) {
				if time.Since(item.usedTime) > p.MaxUsage {
					_ = p.closeList(true, item)
					p.delayClose.Remove(key)
				}
			})
		}
	}
}

func (p *CommPool[T]) closeList(nowClose bool, list ...*resInfo[T]) error {
	var retErr error
	lo.ForEach(list, func(item *resInfo[T], index int) {
		if p.CloseFunc != nil {
			if nowClose {
				err := p.CloseFunc(item.Resource)
				if err != nil {
					fmt.Printf("error closing resource: %v\n", err)
					retErr = err
				}
			} else {
				if cond.IsZero(item.usedTime) {
					item.usedTime = time.Now()
				}
				p.delayClose.Set(item.id, item)
			}
		}
	})
	if retErr != nil {
		return retErr
	}
	return nil
}

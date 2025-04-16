package cache

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/magic-lib/go-plat-utils/id-generator/id"
	"github.com/magic-lib/go-plat-utils/utils"
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

// ConnPool 资源池结构体，传入参数
type ConnPool[T any] struct {
	MaxSize   int               // 最大资源数量
	MaxUsage  time.Duration     // 资源最大使用时间
	New       func() (T, error) // 创建新资源的函数
	CheckFunc func(T) error     // 检查资源有效的函数
	CloseFunc func(T) error     // 关闭资源的函数
}

type commPool[T any] struct {
	ConnPool[T]

	idle []*resInfo[T] // 空闲资源列表
	used []*resInfo[T] // 已使用资源列表
	once []*resInfo[T] // 一次性资源列表
	mu   sync.RWMutex
}

// NewPoolConn 创建一个新的资源池
func NewPoolConn[T any](connPool *ConnPool[T]) (*commPool[T], error) {
	pool := new(commPool[T])
	pool.ConnPool = *connPool
	if pool.MaxSize <= 0 {
		pool.MaxSize = defaultMaxSize
	}
	if pool.MaxUsage == 0 {
		pool.MaxUsage = defaultMaxUsage
	}
	if pool.New == nil {
		return nil, fmt.Errorf("new function is required")
	}
	pool.idle = make([]*resInfo[T], 0, pool.MaxSize)
	pool.used = make([]*resInfo[T], 0, pool.MaxSize)
	pool.once = make([]*resInfo[T], 0)

	// 默认放一个，用来检测是否可以创建
	resource, err := pool.create()
	if err != nil {
		fmt.Println("NewPoolConn error:", err.Error())
	} else {
		pool.idle = append(pool.idle, resource)
	}

	if pool.CheckFunc != nil {
		goroutines.GoAsync(func(params ...any) {
			pool.checkIdleResources()
		}, nil)
	}
	if pool.MaxUsage > 0 {
		goroutines.GoAsync(func(params ...any) {
			pool.checkMaxUsageResources()
		}, nil)
	}

	return pool, nil
}

// Get 从资源池获取一个资源
func (p *commPool[T]) create() (*resInfo[T], error) {
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
func (p *commPool[T]) Get() (*resInfo[T], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.idle == nil || p.used == nil {
		return nil, fmt.Errorf("resource pool is closed")
	}
	//如果存在空闲资源，直接返回
	if len(p.idle) > 0 {
		resource := p.idle[0]
		resource.usedTime = time.Now()
		p.idle = p.idle[1:]
		p.used = append(p.used, resource)
		return resource, nil
	}

	//已经满了，则直接创建，使用短连接
	resource, err := p.create()
	if err != nil {
		return nil, err
	}

	if len(p.used) == p.MaxSize {
		p.once = append(p.once, resource)
	} else {
		p.used = append(p.used, resource)
	}

	return resource, nil
}

// Put 将资源释放回资源池
func (p *commPool[T]) Put(res *resInfo[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()

	//已关闭，直接释放资源
	if p.idle == nil || p.used == nil {
		_ = p.closeList(res)
		return
	}
	usedFind := false
	lo.ForEachWhile(p.used, func(item *resInfo[T], index int) bool {
		if item.id == res.id {
			res.usedTime = time.Time{}
			p.used = append(p.used[:index], p.used[index+1:]...)
			p.idle = append(p.idle, res)
			usedFind = true
			return false
		}
		return true
	})
	// 如果没有找到，可能是一次性资源，从一次性资源列表中移除
	if !usedFind {
		lo.ForEachWhile(p.once, func(item *resInfo[T], index int) bool {
			if item.id == res.id {
				p.once = append(p.once[:index], p.once[index+1:]...)
				//检查idle数量，是否可以重用，超过最大数量，则直接关闭
				if len(p.idle)+len(p.used) < p.MaxSize {
					p.idle = append(p.idle, res)
					return false
				}
				_ = p.closeList(item)
				return false
			}
			return true
		})
	}
}

// Close 关闭资源池中的所有资源
func (p *commPool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.idle == nil || p.used == nil {
		return
	}

	_ = p.closeList(p.idle...)
	_ = p.closeList(p.used...)
	_ = p.closeList(p.once...)

	p.idle = nil
	p.used = nil
	p.once = make([]*resInfo[T], 0)
}

// checkIdleResources 定期检查空闲资源的有效性
func (p *commPool[T]) checkIdleResources() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			errList := make([]*resInfo[T], 0)
			lo.ForEach(p.idle, func(item *resInfo[T], index int) {
				err := p.CheckFunc(item.Resource)
				if err != nil {
					fmt.Printf("error checking idle resource: %v\n", err)
					errList = append(errList, item)
				}
			})
			//无失效情况，则跳过
			if len(errList) == 0 {
				continue
			}
			// 移除无效资源
			p.mu.Lock()

			lo.ForEach(errList, func(item *resInfo[T], _ int) {
				// 尝试创建新资源
				resource, err := p.create()
				if err != nil {
					return
				}
				lo.ForEachWhile(p.idle, func(oldItem *resInfo[T], i int) bool {
					if item.id == oldItem.id {
						// 移除资源
						p.idle = append(p.idle[:i], p.idle[i+1:]...)
						p.idle = append(p.idle, resource)
						if p.CloseFunc != nil { //把失效的关闭
							_ = p.CloseFunc(oldItem.Resource)
						}
						return false
					}
					return true
				})
			})

			p.mu.Unlock()
		}
	}
}

// checkMaxUsageResources 检查超时使用的函数
func (p *commPool[T]) checkMaxUsageResources() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			expiredResources := make([]*resInfo[T], 0)
			lo.ForEach(p.used, func(item *resInfo[T], i int) {
				if time.Since(item.usedTime) > p.MaxUsage {
					expiredResources = append(expiredResources, item)
				}
			})
			lo.ForEach(p.once, func(item *resInfo[T], i int) {
				if time.Since(item.usedTime) > p.MaxUsage {
					expiredResources = append(expiredResources, item)
				}
			})
			if len(expiredResources) == 0 {
				continue
			}
			p.mu.Lock()
			//可能忘记调用Put方法，需要放入空闲列表中
			lo.ForEach(expiredResources, func(item *resInfo[T], i int) {
				p.Put(item)
			})
			p.mu.Unlock()
		}
	}
}

func (p *commPool[T]) closeList(list ...*resInfo[T]) error {
	var retErr error
	lo.ForEach(list, func(item *resInfo[T], index int) {
		if p.CloseFunc != nil {
			err := p.CloseFunc(item.Resource)
			if err != nil {
				fmt.Printf("error closing resource: %v\n", err)
				retErr = err
			}
		}
	})
	if retErr != nil {
		return retErr
	}
	return nil
}

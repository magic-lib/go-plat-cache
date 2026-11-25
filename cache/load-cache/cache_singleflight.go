package cache

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	platcache "github.com/magic-lib/go-plat-cache/cache"
	"golang.org/x/sync/singleflight"
)

// SingleFlight 带泛型的防击穿组件，同一个 key 的并发请求只真正执行一次。
//
//   - 未设置缓存：同一时刻同一 key 只有一个请求执行 fn，其余请求等待并共享这一次的结果。
//   - 设置了缓存：
//     1. 缓存命中：直接返回缓存里的值，同时在后台只让一个请求真正执行 fn 并把新值写回缓存，
//     其余命中的请求不会重复触发刷新；
//     2. 缓存未命中：同未设置缓存，一个请求执行 fn、其余等待共享结果，执行成功后写入缓存。
//
// 缓存的 Get 返回错误或零值都视为未命中。多个请求共享同一个结果，V 为指针、map、切片时调用方不要修改它。
// 可用 WithRefreshInterval 限制缓存命中时的后台刷新频率。
type SingleFlight[V any] struct {
	group           singleflight.Group
	cache           platcache.CommCache[V]
	ttl             time.Duration
	refreshInterval time.Duration
	lastRefresh     sync.Map // key -> *atomic.Int64，上一次加载的时间（UnixNano）
	errorHandler    func(key string, err error)
}

// SingleFlightOption SingleFlight 的可选配置
type SingleFlightOption[V any] func(*SingleFlight[V])

// WithCache 设置缓存和写入缓存的过期时间；不设置则只做并发合并，不缓存结果
func WithCache[V any](c platcache.CommCache[V], ttl time.Duration) SingleFlightOption[V] {
	return func(s *SingleFlight[V]) {
		s.cache = c
		s.ttl = ttl
	}
}

// WithRefreshInterval 设置缓存命中时后台刷新的最小间隔：距离该 key 上一次加载不到 interval 时，
// 只返回缓存值、不触发刷新，即 interval 内同一 key 最多刷新一次。
// 不设置或设置为 <= 0 时，每次命中缓存都会尝试刷新（同一时刻仍只有一个在执行）。
// 刷新失败后同样要等 interval 才会再次刷新。仅在设置了缓存时生效。
// interval 必须小于缓存时间 ttl，否则缓存过期前不会触发刷新；不满足时 NewSingleFlight 会把它调整为 ttl 的一半，
// interval 或 ttl 任一为 0 时不做校验。
func WithRefreshInterval[V any](interval time.Duration) SingleFlightOption[V] {
	return func(s *SingleFlight[V]) {
		s.refreshInterval = interval
	}
}

// WithErrorHandler 设置无法返回给调用方的错误的处理函数：后台刷新失败、写入缓存失败
func WithErrorHandler[V any](handler func(key string, err error)) SingleFlightOption[V] {
	return func(s *SingleFlight[V]) {
		s.errorHandler = handler
	}
}

// NewSingleFlight 创建防击穿组件
func NewSingleFlight[V any](opts ...SingleFlightOption[V]) *SingleFlight[V] {
	s := &SingleFlight[V]{}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	// 刷新间隔不小于缓存时间时，缓存过期前永远不会后台刷新，过期后请求只能同步等待加载，
	// 这里调整为缓存时间的一半；两者任一为 0（缓存不过期 / 不限制刷新间隔）时不做校验
	if s.cache != nil && s.ttl > 0 && s.refreshInterval > 0 && s.refreshInterval >= s.ttl {
		s.refreshInterval = s.ttl / 2
	}
	return s
}

// Do 按 key 执行 fn，同一 key 的并发请求只执行一次，行为见 SingleFlight 的说明。
// 未命中缓存时，fn 使用第一个进来的请求的 ctx 执行，该 ctx 被取消会影响所有等待它结果的请求。
func (s *SingleFlight[V]) Do(ctx context.Context, key string, fn func(ctx context.Context) (V, error)) (V, error) {
	if fn == nil || key == "" {
		var zero V
		return zero, fmt.Errorf("singleflight fn is nil, key: %s", key)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if s.cache != nil {
		if val, ok := s.getFromCache(ctx, key); ok {
			// 命中：先返回旧值，后台只让一个请求去刷新；后台刷新不受当前请求 ctx 取消的影响
			if s.claimRefresh(key) {
				s.group.DoChan(key, func() (any, error) {
					return s.load(context.WithoutCancel(ctx), key, fn, true)
				})
			}
			return val, nil
		}
	}

	ret, err, _ := s.group.Do(key, func() (any, error) {
		return s.load(ctx, key, fn, false)
	})
	if err != nil {
		var zero V
		return zero, err
	}
	val, _ := ret.(V)
	return val, nil
}

// Forget 让 key 的下一次请求重新执行，不再等待当前正在执行的那一次，也不受刷新间隔限制
func (s *SingleFlight[V]) Forget(key string) {
	s.group.Forget(key)
	s.lastRefresh.Delete(key)
}

// claimRefresh 判断缓存命中时是否需要后台刷新：距离上一次加载已超过刷新间隔，
// 并用 CAS 抢占本次刷新，保证并发请求在一个间隔内只有一个能触发刷新
func (s *SingleFlight[V]) claimRefresh(key string) bool {
	if s.refreshInterval <= 0 {
		return true
	}
	now := time.Now().UnixNano()
	last := s.refreshTime(key)
	prev := last.Load()
	if prev != 0 && now-prev < int64(s.refreshInterval) {
		return false
	}
	return last.CompareAndSwap(prev, now)
}

// markLoaded 记录 key 的加载时间，未命中时同步加载的结果也算一次刷新
func (s *SingleFlight[V]) markLoaded(key string) {
	if s.refreshInterval <= 0 {
		return
	}
	s.refreshTime(key).Store(time.Now().UnixNano())
}

func (s *SingleFlight[V]) refreshTime(key string) *atomic.Int64 {
	if v, ok := s.lastRefresh.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := s.lastRefresh.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// load 真正执行 fn，成功后写入缓存。
// background 为 true 表示后台刷新：panic 会被转成错误交给 errorHandler，避免后台 goroutine 让进程崩溃
func (s *SingleFlight[V]) load(ctx context.Context, key string, fn func(ctx context.Context) (V, error), background bool) (ret any, err error) {
	if background {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("singleflight refresh panic, key: %s: %v", key, r)
			}
			if err != nil {
				s.handleError(key, err)
			}
		}()
	}

	val, err := fn(ctx)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		if _, setErr := s.cache.Set(ctx, key, val, s.ttl); setErr != nil {
			// 写缓存失败不影响本次结果，只上报
			s.handleError(key, fmt.Errorf("singleflight set cache error, key: %s: %w", key, setErr))
		} else if !background {
			// 同步加载刚写入的新值，刷新间隔内命中不必再刷新；后台刷新的时间已在 claimRefresh 中记录
			s.markLoaded(key)
		}
	}
	return val, nil
}

// getFromCache 读缓存：返回错误或零值都视为未命中
func (s *SingleFlight[V]) getFromCache(ctx context.Context, key string) (V, bool) {
	val, err := s.cache.Get(ctx, key)
	if err != nil || isZeroValue(val) {
		var zero V
		return zero, false
	}
	return val, true
}

func (s *SingleFlight[V]) handleError(key string, err error) {
	if s.errorHandler != nil {
		s.errorHandler(key, err)
	}
}

// isZeroValue 判断泛型值是否为零值（nil 指针、空字符串、0 等）
func isZeroValue[V any](val V) bool {
	return reflect.ValueOf(&val).Elem().IsZero()
}

package cache

import (
	"context"
	"fmt"
	"reflect"
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
type SingleFlight[V any] struct {
	group        singleflight.Group
	cache        platcache.CommCache[V]
	ttl          time.Duration
	errorHandler func(key string, err error)
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
	return s
}

// Do 按 key 执行 fn，同一 key 的并发请求只执行一次，行为见 SingleFlight 的说明。
// 未命中缓存时，fn 使用第一个进来的请求的 ctx 执行，该 ctx 被取消会影响所有等待它结果的请求。
func (s *SingleFlight[V]) Do(ctx context.Context, key string, fn func(ctx context.Context) (V, error)) (V, error) {
	if fn == nil {
		var zero V
		return zero, fmt.Errorf("singleflight fn is nil, key: %s", key)
	}

	if s.cache != nil {
		if val, ok := s.getFromCache(ctx, key); ok {
			// 命中：先返回旧值，后台只让一个请求去刷新；后台刷新不受当前请求 ctx 取消的影响
			s.group.DoChan(key, func() (any, error) {
				return s.load(context.WithoutCancel(ctx), key, fn, true)
			})
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

// Forget 让 key 的下一次请求重新执行，不再等待当前正在执行的那一次
func (s *SingleFlight[V]) Forget(key string) {
	s.group.Forget(key)
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

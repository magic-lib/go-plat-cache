package cache

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

// CommCache 公共缓存接口
type CommCache[V any] interface {
	Get(ctx context.Context, key string) (V, error)
	Set(ctx context.Context, key string, val V, timeout time.Duration) (bool, error)
	Del(ctx context.Context, key string) (bool, error)
}

// GetNsKey 获取namespace下的key，规范化
func getNsKey(ns string, key string) string {
	if ns != "" {
		return fmt.Sprintf("{%s}%s", ns, key)
	}
	return key
}

func NsGetStr[V any](ctx context.Context, co CommCache[string], ns string, key string) (V, error) {
	retStr, err := co.Get(ctx, getNsKey(ns, key))
	if err != nil {
		var zero V
		return zero, err
	}
	return strToVal[V](retStr)
}
func NsSetStr[V any](ctx context.Context, co CommCache[string], ns string, key string, val V, timeout time.Duration) (bool, error) {
	return co.Set(ctx, getNsKey(ns, key), conv.String(val), timeout)
}

// NsGet xxx
func NsGet[V any](ctx context.Context, co CommCache[V], ns string, key string) (V, error) {
	return co.Get(ctx, getNsKey(ns, key))
}

// NsSet xxx
func NsSet[V any](ctx context.Context, co CommCache[V], ns string, key string, val V, timeout time.Duration) (bool, error) {
	return co.Set(ctx, getNsKey(ns, key), val, timeout)
}

// NsDel xxx
func NsDel[V any](ctx context.Context, co CommCache[V], ns string, key string) (bool, error) {
	return co.Del(ctx, getNsKey(ns, key))
}

var (
	_ CommCache[any] = (*defaultCache[any])(nil)
	_ CommCache[any] = (*redisCache[any])(nil)
	_ CommCache[any] = (*memGoCache[any])(nil)
	_ CommCache[any] = (*memLruCache[any])(nil)
	_ CommCache[any] = (*diskCache[any])(nil)
	_ CommCache[any] = (*mySQLCache[any])(nil)
	_ CommCache[any] = (*JetCache[any])(nil)
)

package httpcache_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache/httpcache"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/magic-lib/go-plat-utils/logs"
	"testing"
	"time"
)

func TestLruCacheMap(t *testing.T) {
	ctx := context.Background()

	goroutines.InitContext(&ctx) // 初始化一下

	htc, err := httpcache.New(&httpcache.Config[string, string]{
		Namespace:               "ccccc",
		Timeout:                 time.Nanosecond,
		MaxSize:                 10,
		Expiration:              0,
		CleanupInterval:         0,
		AsyncExecuteDuration:    0,
		NeedAsyncExecuteHandler: nil,
		GetDataHandler: func(ctx context.Context, cacheKey string, getDataParam string) (string, error) {

			fmt.Println("ExecuteGetDataHandle")
			time.Sleep(5 * time.Second)

			return "mmmmm", nil
		},
	})

	htc.Set(nil, "cccc", "aaaaaaa")

	mmm, err := htc.Get(ctx, "cccc", "")

	logs.CtxLogger(ctx).Info("cccc:", mmm, err)

	return

	//htc, err = New(&Config[string, string]{
	//	NameSpace:              "ccccc",
	//	MaxSize:                2,
	//	Expiration:             0,
	//	CleanupInterval:        0,
	//	AsyncExecuteDuration:   0,
	//	NeedAsyncExecuteHandle: nil,
	//	ExecuteGetDataHandle: func(ctx context.Context, cacheKey string, getDataParam string) (string, error) {
	//		time.Sleep(5 * time.Second)
	//
	//		return "mmmmm", nil
	//	},
	//})
	//
	//htc, err = New(&Config[string, string]{
	//	NameSpace:              "ccccc",
	//	MaxSize:                2,
	//	Expiration:             0,
	//	CleanupInterval:        0,
	//	AsyncExecuteDuration:   0,
	//	NeedAsyncExecuteHandle: nil,
	//	ExecuteGetDataHandle: func(ctx context.Context, cacheKey string, getDataParam string) (string, error) {
	//		time.Sleep(5 * time.Second)
	//
	//		return "mmmmm", nil
	//	},
	//})
	//
	//htc, err = New(&Config[string, string]{
	//	NameSpace:              "ccccc",
	//	MaxSize:                2,
	//	Expiration:             0,
	//	CleanupInterval:        0,
	//	AsyncExecuteDuration:   0,
	//	NeedAsyncExecuteHandle: nil,
	//	ExecuteGetDataHandle: func(ctx context.Context, cacheKey string, getDataParam string) (string, error) {
	//		time.Sleep(1 * time.Second)
	//
	//		return "mmmmm", nil
	//	},
	//})
	//
	//htc.Set(nil, "aaaa", "qqq")
	//htc.Set(nil, "bbbb", "www")
	//htc.Set(nil, "cccc", "eee")
	//htc.Set(nil, "dddd", "fff")
	//
	//mmm, err := htc.Get(nil, "cccc", "")
	//fmt.Println("cccc:", mmm, err)
	//
	//mmm, err = htc.Get(nil, "aaaa", "")
	//fmt.Println("aaaa:", mmm, err)
	//mmm, err = htc.Get(nil, "bbbb", "")
	//fmt.Println("bbbb:", mmm, err)
	//mmm, err = htc.Get(nil, "mmmm", "")
	//fmt.Println("bbbb:", mmm, err)
	//
	//mmm, err = htc.Get(nil, "mmmm", "")
	//fmt.Println("bbbb:", mmm, err)

	//mmm, err = htc.Get(nil, "dddd")
	//fmt.Println("dddd:", mmm, err)
	//mmm, err = htc.Get(nil, "dddd")
	//fmt.Println("dddd:", mmm, err)

}

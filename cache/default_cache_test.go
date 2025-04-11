package cache_test

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/samber/lo"
	"testing"
	"time"
)

type AA struct {
	Name string
}
type BB struct {
	Name string
}

func TestLruCacheMap(t *testing.T) {
	oneCache := cache.New[*AA]("aa", cache.NewMemGoCache[string](10*time.Minute, 10*time.Minute))
	oneCache.Set(nil, "aaaa", &AA{
		Name: "tiantian",
	}, 50*time.Second)

	oneCache1 := cache.New[*BB]("aa", cache.NewMemGoCache[string](10*time.Minute, 10*time.Minute))
	oneCache1.Set(nil, "aaaa", &BB{
		Name: "tiantian2222",
	}, 50*time.Second)

	kk, err := oneCache.Get(nil, "aaaa")
	if kk == nil {
		fmt.Println("kk is nil")
	}
	fmt.Println(kk, err)

	kkk, err := oneCache1.Get(nil, "aaaa")
	if kk == nil {
		fmt.Println("kkk is nil")
	}
	fmt.Println(kkk, err)
}
func TestBatchExec(t *testing.T) {
	client := cache.NewRedisClient(nil)
	cmdList, err := client.BatchExec(context.Background(), func(ctx context.Context, pipe redis.Pipeliner) []redis.Cmder {
		// 批量执行命令
		incr := pipe.Incr(ctx, "pipeline_counter")
		pipe.Expire(ctx, "pipeline_counter", 0)
		get := pipe.Get(ctx, "pipeline_counter")
		return []redis.Cmder{incr, get}
	})
	fmt.Println(err)
	lo.ForEach(cmdList, func(item redis.Cmder, index int) {
		if _, ok := item.(*redis.IntCmd); ok {

		}
	})
}

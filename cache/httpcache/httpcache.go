package httpcache

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/timandy/routine"
	"runtime"
	"time"
)

// Config 配置
type Config[RQ any, RD any] struct {
	Namespace               string                                                                  //全局唯一，保证存储的一类数据，数据分类使用
	CacheList               []cache.CommCache[*cacheData[RD]]                                       //存储的类型，可以有多个，这样可以比如有内存和redis共同存储
	MaxSize                 int                                                                     //存储的最大数量，控制存储数量，避免内存过大
	EvictionType            EvictionPolicy                                                          //未过有效期，超过MaxSize后主动淘汰的策略
	Timeout                 time.Duration                                                           //获取超时时间，有可能硬盘出现问题，存在缓存慢的情况，如果超时，则执行ExecuteGetDataHandle，默认不设置
	Expiration              time.Duration                                                           //数据多长时间过期，过期以后被动淘汰
	CleanupInterval         time.Duration                                                           //间隔过久执行清理，主动清理
	AsyncExecuteDuration    time.Duration                                                           //在这段时间里不执行异步更新，避免瞬时压力
	NeedAsyncExecuteHandler func(ctx context.Context, responseData RD) bool                         //这个数据是否需要自动异步更新
	GetDataHandler          func(ctx context.Context, cacheKey string, requestParam RQ) (RD, error) //动态获取数据
}

func New[RQ any, RD any](cfg *Config[RQ, RD]) (HttpCache[RQ, RD], error) {
	fileName := ""
	fileLine := 0
	if cfg.Namespace == "" {
		_, fileName, fileLine, _ = runtime.Caller(1)
	}
	err := cfg.checkParam(fileName, fileLine)
	if err != nil {
		return nil, fmt.Errorf("httpCache New error:"+cfg.Namespace+": %s", err.Error())
	}

	n := new(cacheIns[RQ, RD])
	n.cfg = cfg

	return n, err
}

// Close 关闭httpCache
func Close() {
	closed = true
}

// Get 获取一个对象
func (c *cacheIns[RQ, RD]) Get(ctx context.Context, cacheKey string, requestParam RQ) (value RD, err error) {
	a := routine.Goid()
	fmt.Println(a)

	retMap, err := c.multiGetData(ctx, map[string]RQ{
		cacheKey: requestParam,
	})

	if err != nil {
		return value, err
	}
	if data, ok := retMap[cacheKey]; ok {
		return data, nil
	}
	return value, nil
}

// Set 外部手动进行设置
func (c *cacheIns[RQ, RD]) Set(ctx context.Context, cacheKey string, responseData RD) bool {
	if cond.IsNil(responseData) || cacheKey == "" {
		return false
	}
	ret, err := multiSetData(ctx, c.cfg.CacheList, c.cfg.Namespace, cacheKey, responseData, c.cfg.Expiration)
	if err != nil {
		return false
	}
	return ret
}

// Del 删除一个对象
func (c *cacheIns[RQ, RD]) Del(ctx context.Context, cacheKey string) bool {
	if cacheKey == "" {
		return false
	}
	ret, err := multiDelData(ctx, c.cfg.CacheList, c.cfg.Namespace, cacheKey)
	if err != nil {
		return false
	}
	return ret
}

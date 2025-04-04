package httpcache

import (
	"context"
)

type EvictionPolicy int

const (
	LRUPolicy EvictionPolicy = iota
	FIFOPolicy
	LFUPolicy
	RandomPolicy
)

// HttpCache 获取某一个数据的接口
type HttpCache[RQ any, RD any] interface {
	Get(ctx context.Context, cacheKey string, requestParam RQ) (RD, error)
	Set(ctx context.Context, cacheKey string, responseData RD) bool
	Del(ctx context.Context, cacheKey string) bool
}

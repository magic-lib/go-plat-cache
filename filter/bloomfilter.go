package filter

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/hugh2632/bloomfilter"
	"github.com/hugh2632/bloomfilter/global"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type BloomFilterOption struct {
	ByteLen        uint64 //字节长度
	Hashes         []global.HashFunc
	FilterInstance bloomfilter.IFilter //自定义
	RedisFilter    *struct {
		RedisOptions *redis.Options
		FilterType   bloomfilter.RedisFilterType
		Key          string
	}
	SqlFilter *struct {
		SqlDSN string
		SqlDb  *gorm.DB
		Key    string
	}
}

const (
	defaultKey     = "bloom-filter"
	defaultByteLen = 10240
)

type BloomFilter struct {
	instance bloomfilter.IFilter
}

func NewBloomFilter(bo *BloomFilterOption) (*BloomFilter, error) {
	bo = initOption(bo)
	if bo.RedisFilter != nil && bo.RedisFilter.RedisOptions != nil {
		cli := redis.NewClient(bo.RedisFilter.RedisOptions)
		redisFilter, err := bloomfilter.NewRedisFilter(context.Background(), cli,
			bo.RedisFilter.FilterType, bo.RedisFilter.Key, bo.ByteLen, bo.Hashes...)
		if err != nil {
			return nil, err
		}
		return &BloomFilter{
			instance: redisFilter,
		}, nil
	}
	if bo.SqlFilter != nil && (bo.SqlFilter.SqlDSN != "" || bo.SqlFilter.SqlDb != nil) {
		if bo.SqlFilter.SqlDb == nil {
			var err error
			bo.SqlFilter.SqlDb, err = gorm.Open(mysql.Open(bo.SqlFilter.SqlDSN))
			if err != nil {
				return nil, err
			}
		}

		sqlFilter, err := bloomfilter.SqlFilter(bo.SqlFilter.SqlDb, bo.SqlFilter.Key, bo.ByteLen, bo.Hashes...)
		if err != nil {
			return nil, err
		}
		return &BloomFilter{
			instance: sqlFilter,
		}, nil
	}

	if bo.FilterInstance != nil {
		return &BloomFilter{
			instance: bo.FilterInstance,
		}, nil
	}

	return &BloomFilter{
		instance: bloomfilter.NewMemoryFilter(make([]byte, bo.ByteLen), bo.Hashes...),
	}, nil
}

func initOption(bo *BloomFilterOption) *BloomFilterOption {
	if bo == nil {
		bo = &BloomFilterOption{}
	}
	if bo.ByteLen == 0 {
		bo.ByteLen = defaultByteLen
	}
	if len(bo.Hashes) == 0 {
		bo.Hashes = bloomfilter.DefaultHash
	}

	if bo.RedisFilter != nil && bo.RedisFilter.RedisOptions != nil {
		if bo.RedisFilter.FilterType == 0 {
			bo.RedisFilter.FilterType = bloomfilter.RedisFilterType_Cached
		}
		if bo.RedisFilter.Key == "" {
			bo.RedisFilter.Key = defaultKey
		}
	}

	if bo.SqlFilter != nil && (bo.SqlFilter.SqlDSN != "" || bo.SqlFilter.SqlDb != nil) {
		if bo.SqlFilter.Key == "" {
			bo.SqlFilter.Key = defaultKey
		}
	}

	return bo
}

func (bf *BloomFilter) Get() bloomfilter.IFilter {
	return bf.instance
}

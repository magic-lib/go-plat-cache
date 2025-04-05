package cache

import (
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
	"time"
)

var (
	redisMap       = cmap.New[*redisClient]()
	once           sync.Once
	defaultManager *redisClientManager
)

type redisClientManager struct {
}

func NewRedisClientManager(interval time.Duration) *redisClientManager {
	once.Do(func() {
		go monitorRedisConnections(interval)
	})
	if defaultManager != nil {
		return defaultManager
	}
	defaultManager = &redisClientManager{}
	return defaultManager
}

// add 向全局客户端列表添加 Redis 客户端
func (r *redisClientManager) add(redisCfg *startupcfg.RedisConfig) bool {
	if redisCfg == nil {
		return false
	}
	redisConnStr := redisCfg.DatasourceName()
	if redisConnStr == "" {
		return false
	}
	if redisMap.Has(redisConnStr) {
		return false
	}
	newRedisClient := &redisClient{
		redisCfg: redisCfg,
		cli:      nil,
	}
	newClient, err := getRedisFromCfg(redisCfg)
	if err == nil {
		newRedisClient.cli = newClient
	}
	redisMap.Set(redisConnStr, newRedisClient)
	return true
}

// Get 向全局客户端列表添加 Redis 客户端
func (r *redisClientManager) Get(redisCfg *startupcfg.RedisConfig) *redisClient {
	if redisCfg == nil {
		return nil
	}
	redisConnStr := redisCfg.DatasourceName()
	if redisConnStr == "" {
		return nil
	}
	if newRedisClient, ok := redisMap.Get(redisConnStr); ok {
		if newRedisClient.cli == nil {
			newClient, err := getRedisFromCfg(redisCfg)
			if err != nil {
				return nil
			}
			newRedisClient.cli = newClient
		}
		return newRedisClient
	}

	if !r.add(redisCfg) {
		return nil
	}
	return r.Get(redisCfg)
}

// monitorRedisConnections 定时监测所有 Redis 连接状态
func monitorRedisConnections(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, rc := range redisMap.Items() {
				if rc.cli != nil {
					err := checkConnection(rc.cli, rc.redisCfg.PingTimeout)
					if err == nil {
						continue
					}
				}
				newClient, err := getRedisFromCfg(rc.redisCfg)
				if err != nil {
					// TODO 连接失败，20s后尝试重新连接
					continue
				}
				if rc.cli != nil {
					_ = rc.cli.Close() //老的连接需要释放掉
				}
				rc.cli = newClient
			}
		}
	}
}

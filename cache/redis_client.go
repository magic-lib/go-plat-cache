package cache

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/logs"
	"time"
)

var (
	minMaxTimeout     = 24 * time.Hour      //最小的最长时间
	redisMaxTimeout   = 24 * 90 * time.Hour //redis最长存储时间点，避免无限期占用Redis空间
	checkConnInterval = 20 * time.Second
)

// redisClient 内部redis结构
type redisClient struct {
	redisCfg *startupcfg.RedisConfig
	cli      *redis.Client
}

// NewRedisClient 新建redis连接
func NewRedisClient(redisCfg *startupcfg.RedisConfig) *redisClient {
	return &redisClient{redisCfg: redisCfg}
}

// SetMaxTimeout xxx
func (r *redisClient) SetMaxTimeout(timeout time.Duration) {
	if timeout > minMaxTimeout { //必须大于一天，设置过短的时间点会出现问题
		redisMaxTimeout = timeout
	}
}

// NsGet xxx
func (r *redisClient) NsGet(ctx context.Context, ns string, key string) (string, error) {
	return r.Get(ctx, getNsKey(ns, key))
}

// NsSet xxx
func (r *redisClient) NsSet(ctx context.Context, ns string, key, val string, timeout time.Duration) (bool, error) {
	return r.Set(ctx, getNsKey(ns, key), val, timeout)
}

// NsDel xxx
func (r *redisClient) NsDel(ctx context.Context, ns string, key string) (bool, error) {
	return r.Del(ctx, getNsKey(ns, key))
}

// NsHGet xxx
func (r *redisClient) NsHGet(ctx context.Context, ns string, key, field string) (string, error) {
	return r.HGet(ctx, getNsKey(ns, key), field)
}

// NsHSet xxx
func (r *redisClient) NsHSet(ctx context.Context, ns string, key, field, val string, timeout time.Duration) (bool, error) {
	return r.HSet(ctx, getNsKey(ns, key), field, val, timeout)
}

// NsHDel xxx
func (r *redisClient) NsHDel(ctx context.Context, ns string, key, field string) (bool, error) {
	return r.HDel(ctx, getNsKey(ns, key), field)
}

// Get 从缓存中取得一个值，如果没有redis则从本地缓存
func (r *redisClient) Get(ctx context.Context, key string) (string, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return "", err
	}
	var rep string
	rep, err = c.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return rep, nil
}

// Set timeout
func (r *redisClient) Set(ctx context.Context, key, val string, timeout time.Duration) (bool, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return false, err
	}

	if timeout <= 0 || timeout > redisMaxTimeout {
		//设置一个有效的时间点
		timeout = redisMaxTimeout
	}

	err = c.Set(ctx, key, val, timeout).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Del 从缓存中删除一个key
func (r *redisClient) Del(ctx context.Context, key string) (bool, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return false, err
	}
	err = c.Del(ctx, key).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// HSet 设置
func (r *redisClient) HSet(ctx context.Context, key, field string, value string, timeout time.Duration) (bool, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return false, err
	}

	if timeout <= 0 || timeout > redisMaxTimeout {
		//设置一个有效的时间点
		timeout = redisMaxTimeout
	}

	err = c.HSet(ctx, key, field, value).Err()
	if err != nil {
		return false, err
	}

	c.Expire(ctx, key, timeout)

	return true, nil
}

// HGet 获取
func (r *redisClient) HGet(ctx context.Context, key, field string) (string, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return "", err
	}

	retStr, err := c.HGet(ctx, key, field).Result()
	if err != nil {
		return "", err
	}
	return retStr, nil
}

// HDel redis删除
func (r *redisClient) HDel(ctx context.Context, key, field string) (bool, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return false, err
	}

	err = c.HDel(ctx, key, field).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// BatchExec 批量执行
func (r *redisClient) BatchExec(ctx context.Context, f func(ctx context.Context, pipe redis.Pipeliner) []redis.Cmder) ([]redis.Cmder, error) {
	c, err := r.getClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis getClient error: %w", err)
	}
	pipe := c.Pipeline()
	cmdList := f(ctx, pipe)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis pipe Exec error: %w", err)
	}
	return cmdList, nil
}

func (r *redisClient) CheckConnect() bool {
	_, err := r.getOneRedis()
	if err == nil {
		return true
	}
	return false
}

func (r *redisClient) getClient(_ context.Context) (*redis.Client, error) {
	cli, err := r.getOneRedis()
	if cli != nil && err == nil {
		return cli, nil
	}

	loggers := logs.DefaultLogger()
	if r.redisCfg != nil {
		// 如果未设置redis，则提示
		loggers.Error("[redis-client] error:", r.redisCfg, err.Error())
	} else {
		// 没有设置，全局只提醒一次
		onceError.Do(func() {
			loggers.Warn("[redis-client] no set empty:", err.Error())
		})
	}
	return nil, err
}

func (r *redisClient) getOneRedis() (*redis.Client, error) {
	manager := NewRedisClientManager(checkConnInterval)
	rc := manager.Get(r.redisCfg)
	if rc != nil && rc.cli != nil {
		if defaultRedisCfg == nil {
			SetDefaultRedisConfig(r.redisCfg)
		}
		return rc.cli, nil
	}

	return nil, fmt.Errorf("conn cant connect")
}

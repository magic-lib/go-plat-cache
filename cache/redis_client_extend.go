package cache

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"runtime"
	"sync"
	"time"
)

var (
	onceError sync.Once

	defaultPingTimeout = 3 * time.Second

	poolMaxSize = 100
	poolMinSize = 10

	poolMinIdleConns = 30            //连接池中最小的空闲连接数，可以通过此属性提供更快的连接分配，默认为0
	poolMaxConnAge   = 3 * time.Hour //Redis 连接的最大寿命，在连接池中的连接达到最大寿命时，客户端会将连接归还到连接池中，
	// 从而避免连接长时间占用资源。默认为不限制连接寿命
	poolPoolTimeout time.Duration = 0 //当连接池中所有连接均被占用时，客户端调用连接池中连接的 Get() 方法会等待的最长时间。
	// 默认值为 ReadTimeout 加上1秒
	poolIdleTimeout = 5 * time.Minute //Redis 连接在空闲状态下的最长存活时间，超过该时间的连接将被关闭。如果指定的值小于服务器上
	// 的超时时间，则客户端在检查连接空闲时会关闭连接，以防止服务器出现连接超时。默认为5分钟。将其设为-1可以禁用连接空闲超时检查
	poolIdleCheckFrequency = time.Minute //空闲连接检查频率。默认为1分钟。将其设为-1可以禁用连接空闲超时检查器，但是仍然会
)

func checkConnection(conn *redis.Client, pingTimeout time.Duration) error {
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}

	timeout := defaultPingTimeout
	if pingTimeout > 0 {
		timeout = pingTimeout
	}

	newCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return conn.Ping(newCtx).Err()
}

func getRedisFromCfg(redisCfg *startupcfg.RedisConfig) (*redis.Client, error) {
	dialOpt := getRedisOption(redisCfg, getPoolSize())
	newClient := redis.NewClient(dialOpt)
	err := checkConnection(newClient, redisCfg.PingTimeout)
	if err != nil {
		_ = newClient.Close()
		return nil, err
	}
	return newClient, nil
}

func getRedisOption(redisCfg startupcfg.Database, poolSize int) *redis.Options {
	dialOpt := &redis.Options{}
	if dataInt, ok := conv.Int64(redisCfg.DatabaseName()); ok {
		dialOpt.DB = int(dataInt)
	}
	dialOpt.Username = redisCfg.User()
	dialOpt.Password = redisCfg.Password()

	if oneTls, ok := redisCfg.Extend("tls"); ok {
		tlsBool, ok := conv.Bool(oneTls)
		if ok && tlsBool {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
			}
			if tlsConfig.ServerName == "" {
				tlsConfig.ServerName = redisCfg.ServerAddress()
			}
			dialOpt.TLSConfig = tlsConfig
		}
	}

	dialOpt.Addr = redisCfg.ServerAddress()
	dialOpt.Network = redisCfg.ProtocolName()

	{ // 连接池的配置
		dialOpt.PoolFIFO = true                 //Redis 连接池是否使用 FIFO 先进先出的连接池类型，默认为 true
		dialOpt.PoolSize = poolSize             //连接池中最多能同时存放的 Redis 连接数，即最大连接数
		dialOpt.MinIdleConns = poolMinIdleConns //连接池中最小的空闲连接数，可以通过此属性提供更快的连接分配，默认为0
		dialOpt.MaxConnAge = poolMaxConnAge     //Redis 连接的最大寿命，在连接池中的连接达到最大寿命时，客户端会将连接归还到连接池中，
		// 从而避免连接长时间占用资源。默认为不限制连接寿命
		dialOpt.PoolTimeout = poolPoolTimeout //当连接池中所有连接均被占用时，客户端调用连接池中连接的 Get() 方法会等待的最长时间。
		// 默认值为 ReadTimeout 加上1秒
		dialOpt.IdleTimeout = poolIdleTimeout //Redis 连接在空闲状态下的最长存活时间，超过该时间的连接将被关闭。如果指定的值小于服务器上
		// 的超时时间，则客户端在检查连接空闲时会关闭连接，以防止服务器出现连接超时。默认为5分钟。将其设为-1可以禁用连接空闲超时检查
		dialOpt.IdleCheckFrequency = poolIdleCheckFrequency //空闲连接检查频率。默认为1分钟。将其设为-1可以禁用连接空闲超时检查器，但是仍然会
		// 根据 IdleTimeout 的值关闭空闲连接。
	}
	return dialOpt
}

func getPoolSize() int {
	poolSize := runtime.GOMAXPROCS(0)
	if poolSize < poolMinSize {
		poolSize = poolMinSize
	}
	if poolSize > poolMaxSize {
		poolSize = poolMaxSize
	}
	return poolSize
}

// 取得默认的ctx
func getContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	ctxPtr, _, _ := goroutines.GetContext()
	if ctxPtr == nil {
		ctxOne := context.Background()
		ctxPtr = &ctxOne
	}
	return *ctxPtr
}

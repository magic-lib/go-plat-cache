package redisclient

import (
	"crypto/tls"
	"fmt"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/syncx"
	red "github.com/redis/go-redis/v9"
	"io"
	"runtime"
	"strings"
)

const (
	// ClusterType means redis cluster.
	ClusterType = "cluster"
	// NodeType means redis node.
	NodeType = "node"

	addrSep         = ","
	defaultDatabase = 0
	maxRetries      = 3
	idleConns       = 8
)

var (
	clientManager = syncx.NewResourceManager()
	// nodePoolSize is default pool size for node type of redis.
	nodePoolSize = 10 * runtime.GOMAXPROCS(0)

	clusterManager = syncx.NewResourceManager()
)

func getClient(r *startupcfg.RedisConfig) (*red.Client, error) {
	val, err := clientManager.GetResource(r.DatasourceName(), func() (io.Closer, error) {
		var tlsConfig *tls.Config
		if r.TLS {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		db, _ := conv.Convert[int64](r.DatabaseName())
		store := red.NewClient(&red.Options{
			Addr:         r.ServerAddress(),
			Username:     r.User(),
			Password:     r.Password(),
			DB:           int(db),
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
			TLSConfig:    tlsConfig,
		})

		return store, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*red.Client), nil
}

func getCluster(r *startupcfg.RedisConfig) (*red.ClusterClient, error) {
	val, err := clusterManager.GetResource(r.DatasourceName(), func() (io.Closer, error) {
		var tlsConfig *tls.Config
		if r.TLS {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		store := red.NewClusterClient(&red.ClusterOptions{
			Addrs:        splitClusterAddrs(r.ServerAddress()),
			Username:     r.User(),
			Password:     r.Password(),
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
			TLSConfig:    tlsConfig,
		})

		return store, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*red.ClusterClient), nil
}

func splitClusterAddrs(addr string) []string {
	addrs := strings.Split(addr, addrSep)
	unique := make(map[string]struct{})
	for _, each := range addrs {
		unique[strings.TrimSpace(each)] = struct{}{}
	}

	addrs = addrs[:0]
	for k := range unique {
		addrs = append(addrs, k)
	}

	return addrs
}

func getRedis(r *startupcfg.RedisConfig) (any, error) {
	switch r.Type {
	case ClusterType:
		return getCluster(r)
	case NodeType:
		return getClient(r)
	default:
		return nil, fmt.Errorf("redis type '%s' is not supported", r.Type)
	}
}

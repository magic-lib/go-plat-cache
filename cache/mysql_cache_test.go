package cache_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"testing"
)

type AAA struct {
	Name string
}

func TestMySQLCache(t *testing.T) {
	mysqlCacheTemp, err := cache.NewMySQLCache[AAA](&cache.MySQLCacheConfig{
		DSN:       "root:xxxxx@(127.0.0.1:3306)/huji?charset=utf8mb4&parseTime=True&loc=Local",
		TableName: "aaa",
		Namespace: "ns",
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	retBool, err := mysqlCacheTemp.Set(context.Background(), "key1", AAA{
		Name: "tianlin0",
	}, 0)

	fmt.Println(retBool, err)

	mmm, err := mysqlCacheTemp.Get(context.Background(), "key1")
	fmt.Println(mmm, err)
}

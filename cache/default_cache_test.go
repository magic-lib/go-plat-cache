package cache_test

import (
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
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

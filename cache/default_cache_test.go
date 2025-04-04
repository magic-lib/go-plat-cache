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

func TestLruCacheMap(t *testing.T) {
	oneCache := cache.New[*AA]("aa", cache.NewMemGoCache[string](10*time.Minute, 10*time.Minute))
	oneCache.Set(nil, "aaaa", &AA{
		Name: "tiantian",
	}, 50*time.Second)

	kk, err := oneCache.Get(nil, "aaaaa")
	if kk == nil {
		fmt.Println("kk is nil")
	}
	fmt.Println(kk, err)
}

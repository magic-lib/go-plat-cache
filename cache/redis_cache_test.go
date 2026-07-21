package cache_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"testing"
	"time"
)

func TestRedisCache(t *testing.T) {
	oneRedisConfig := &startupcfg.RedisConfig{
		Address:         "",
		PasswordEncoded: startupcfg.Encrypted(""),
		TLS:             false,
		Type:            "node",
	}
	zamloan2MemberCache, err := cache.NewRedisCache[string](oneRedisConfig)
	if err != nil {
		t.Error(err)
		return
	}
	if zamloan2MemberCache == nil {
		t.Error("zamloan2MemberCache is nil")
		return
	}
	aa, err := zamloan2MemberCache.Get(context.Background(), "key1")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(aa)
	zamloan2MemberCache.Set(context.Background(), "key1", "value1", 5*time.Minute)
}

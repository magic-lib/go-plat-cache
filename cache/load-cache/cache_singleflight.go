package cache

import "golang.org/x/sync/singleflight"

// groupWrapper 用于包装 singleflight.Group，防止重复加载。
type groupWrapper struct {
	group singleflight.Group
}

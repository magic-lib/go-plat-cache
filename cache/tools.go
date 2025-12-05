package cache

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/conv"
	"reflect"
)

func strToVal[V any](valueStr string) (V, error) {
	var value V
	newValuePtr := conv.NewPtrByType(reflect.TypeOf(value))
	if err := conv.Unmarshal(valueStr, newValuePtr); err != nil {
		var zero V
		return zero, fmt.Errorf("反序列化缓存值失败: %v", err)
	}
	if v, ok := newValuePtr.(V); ok {
		return v, nil
	}
	if ptr, ok := newValuePtr.(*V); ok {
		return *ptr, nil
	}
	return value, nil
}

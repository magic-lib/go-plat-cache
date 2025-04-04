package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/magic-lib/go-plat-utils/conv"
	"time"
)

type defaultCache[T any] struct {
	cCache     CommCache[string]
	ns         string // 命名空间
	isMemCache bool   //是否是默认的，避免重复提交
}

var (
	defaultMemCache = NewMemGoCache[any](5*time.Minute, 10*time.Minute) //本地默认缓存
)

// New 新建
func New[T any](ns string, con ...CommCache[string]) *defaultCache[T] {
	com := new(defaultCache[T])
	com.ns = ns
	if len(con) > 0 {
		com.isMemCache = false
		com.cCache = con[0]
		return com
	}
	com.isMemCache = true
	return com
}

// Get 从缓存中取得一个值，如果没有redis则从本地缓存
func (co *defaultCache[T]) Get(ctx context.Context, key string) (T, error) {
	key = getNsKey(co.ns, key)
	return co.getOne(ctx, key)
}

// Set timeout为秒
func (co *defaultCache[T]) Set(ctx context.Context, key string, val T, timeout time.Duration) (bool, error) {
	key = getNsKey(co.ns, key)
	return co.setOne(ctx, key, val, timeout)
}

// Del 从缓存中删除一个key，同时删除
func (co *defaultCache[T]) Del(ctx context.Context, key string) (bool, error) {
	key = getNsKey(co.ns, key)
	return co.delOne(ctx, key)
}

func (co *defaultCache[T]) getOne(ctx context.Context, key string) (T, error) {
	ret2, err2 := defaultMemCache.Get(ctx, key)
	if err2 == nil {
		if retVal, ok := ret2.(T); ok {
			return retVal, nil
		}
	}

	if co.isMemCache || co.cCache == nil {
		return *new(T), err2
	}

	ret, err := co.cCache.Get(ctx, key)
	if err != nil {
		return *new(T), err
	}

	return decodeValue[T](ret)
}

func decodeValue[T any](val string) (T, error) {
	if val == "" {
		return *new(T), nil
	}

	var buf bytes.Buffer
	buf.WriteString(val)
	enc := gob.NewDecoder(&buf)
	newT := new(T)
	err := enc.Decode(newT)
	if err == nil {
		return *newT, nil
	}
	err = conv.Unmarshal(val, newT)
	if err != nil {
		return *newT, err
	}
	return *newT, nil
}

func encodeValue[T any](val T) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	var saveStr string
	if err != nil {
		saveStr = conv.String(val)
	} else {
		saveStr = string(buf.Bytes())
	}
	return saveStr
}

func (co *defaultCache[T]) setOne(ctx context.Context, key string, val T, timeout time.Duration) (bool, error) {
	var retBool, ret, ret2 bool
	var retError, err, err2 error
	if co.cCache != nil {
		saveStr := encodeValue[T](val)
		ret, err = co.cCache.Set(ctx, key, saveStr, timeout)
		if err == nil && ret {
			return true, nil
		}
	}
	//如果没有保存成功，则用内存保存一遍
	ret2, err2 = defaultMemCache.Set(ctx, key, val, timeout)
	if co.isMemCache {
		retBool = ret2
		retError = err2
	} else {
		retBool = ret
		retError = err
	}
	return retBool, retError
}

func (co *defaultCache[T]) delOne(ctx context.Context, key string) (bool, error) {
	var retBool, ret, ret2 bool
	var retError, err, err2 error
	ret2, err2 = defaultMemCache.Del(ctx, key)
	if co.cCache != nil {
		ret, err = co.cCache.Del(ctx, key)
	}
	if co.isMemCache {
		retBool = ret2
		retError = err2
	} else {
		retBool = ret
		retError = err
	}
	return retBool, retError
}

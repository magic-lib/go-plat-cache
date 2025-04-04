// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

// Package gmlock implements a concurrent-safe memory-based locker.
package gmlock

import (
	"context"
	"fmt"
)

type MemLock struct {
	key string
}

const (
	DefaultKeyFront = "{mem-lock}"
)

func getLockerKeyName(key string) string {
	return fmt.Sprintf("%s%s", DefaultKeyFront, key)
}

// NewMemLock 新的锁
func NewMemLock(key string) *MemLock {
	return &MemLock{
		key: getLockerKeyName(key),
	}
}

// Lock 上锁
func (m *MemLock) Lock(ctx context.Context) (bool, error) {
	Lock(m.key)
	return true, nil
}

// UnLock 解锁
func (m *MemLock) UnLock(ctx context.Context) (bool, error) {
	Unlock(m.key)
	Remove(m.key)
	return true, nil
}

// TryLock 尝试加锁
func (m *MemLock) TryLock(ctx context.Context) (bool, error) {
	return TryLock(m.key), nil
}

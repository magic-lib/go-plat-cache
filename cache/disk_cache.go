package cache

import (
	"context"
	"fmt"
	"github.com/gregjones/httpcache/diskcache"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"os"
	"path/filepath"
	"time"
)

type DataWithExpiry struct {
	Data   string
	Expiry time.Time
}

const (
	checkInterval        = time.Hour
	defaultMaxExpireTime = 2 * 24 * time.Hour
)

type diskCache[V any] struct {
	diskCache     *diskcache.Cache
	basePath      string
	maxExpireTime time.Duration
}

// NewDiskCache 新建diskCache
func NewDiskCache[V string](basePath string, maxExpireTime time.Duration) *diskCache[string] {
	diskCacheInstance := diskcache.New(basePath)
	if maxExpireTime < defaultMaxExpireTime {
		maxExpireTime = defaultMaxExpireTime
	}
	co := &diskCache[string]{
		basePath:      basePath,
		diskCache:     diskCacheInstance,
		maxExpireTime: maxExpireTime,
	}
	// 启动定时清理任务
	co.autoCleanExpiredFiles()
	return co
}

func (co *diskCache[V]) autoCleanExpiredFiles() {
	goroutines.GoAsync(func(params ...any) {
		cot := params[0].(*diskCache[V])
		for {
			fmt.Printf("\n--- 定时清理触发 (%s) ---\n", conv.String(time.Now()))
			if err := cot.cleanExpiredFiles(co.basePath, co.maxExpireTime); err != nil {
				fmt.Printf("清理错误: %v\n", err)
			}
			<-time.After(checkInterval)
		}
	}, co)
}

// 清理过期文件（包括子目录）
func (co *diskCache[V]) cleanExpiredFiles(rootDir string, maxAge time.Duration) error {
	expiryTime := time.Now().Add(-maxAge)
	deletedCount := 0

	// 递归遍历所有文件和子目录
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("访问路径失败: %s, 错误: %w", path, err)
		}

		// 跳过根目录本身
		if path == rootDir {
			return nil
		}

		// 如果是目录，先不处理（等处理完子文件再判断是否删除空目录）
		if info.IsDir() {
			return nil
		}

		// 检查文件是否过期（使用修改时间判断）
		if info.ModTime().Before(expiryTime) {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("删除文件失败: %s, 错误: %w", path, err)
			}
			deletedCount++
			fmt.Printf("已删除过期文件: %s (修改时间: %s)\n",
				path, info.ModTime().Format("2006-01-02 15:04:05"))
		}
		return nil
	})

	if err != nil {
		return err
	}

	// 二次遍历：删除空目录（可选）
	if deletedCount > 0 {
		err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.IsDir() || path == rootDir {
				return nil
			}

			// 检查目录是否为空
			entries, err := os.ReadDir(path)
			if err != nil {
				return fmt.Errorf("读取目录失败: %s, 错误: %w", path, err)
			}
			if len(entries) == 0 {
				if err := os.Remove(path); err != nil {
					return fmt.Errorf("删除空目录失败: %s, 错误: %w", path, err)
				}
				fmt.Printf("已删除空目录: %s\n", path)
			}
			return nil
		})
	}

	fmt.Printf("本次清理完成，共删除 %d 个过期文件\n", deletedCount)
	return err
}

// Get 从缓存中取得一个值
func (co *diskCache[V]) Get(_ context.Context, key string) (v string, err error) {
	ret, ok := co.diskCache.Get(key)
	if ok {
		var dataWithExpiryRead DataWithExpiry
		err = conv.Unmarshal(ret, &dataWithExpiryRead)
		if err != nil {
			return "", err
		}
		// 判断是否过期
		if time.Now().After(dataWithExpiryRead.Expiry) {
			co.diskCache.Delete(key)
			return "", nil
		}
		return dataWithExpiryRead.Data, nil
	}
	return "", nil
}

// Set timeout无效
func (co *diskCache[V]) Set(_ context.Context, key string, val string, timeout time.Duration) (bool, error) {
	dataWithExpiry := DataWithExpiry{
		Data:   val,
		Expiry: time.Now().Add(timeout),
	}
	serialized := conv.String(dataWithExpiry)
	co.diskCache.Set(key, []byte(serialized))
	return true, nil
}

// Del 从缓存中删除一个key
func (co *diskCache[V]) Del(_ context.Context, key string) (bool, error) {
	co.diskCache.Delete(key)
	return true, nil
}

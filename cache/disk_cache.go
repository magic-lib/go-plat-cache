package cache

import (
	"context"
	"fmt"
	"github.com/gregjones/httpcache/diskcache"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/peterbourgon/diskv"
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

var basePathMap = make(map[string]bool) //避免同一个目录，多个清理程序同时运行

type diskCache[V any] struct {
	diskCache     *diskcache.Cache
	basePath      string
	maxExpireTime time.Duration
}

// NewDiskCache 新建diskCache
func NewDiskCache[V any](basePath string, maxExpireTime time.Duration) CommCache[V] {
	diskCacheInstance := diskcache.NewWithDiskv(diskv.New(diskv.Options{
		BasePath: basePath,
		Transform: func(key string) []string {
			cacheDir := make([]string, 0)
			// 取前2位作为一级目录，接下来2位作为二级目录
			if len(key) >= 4 {
				return append(cacheDir, key[:2], key[2:4])
			}
			return cacheDir
		},
		CacheSizeMax: 100 * 1024 * 1024, // 100MB
	}))
	if maxExpireTime < defaultMaxExpireTime {
		maxExpireTime = defaultMaxExpireTime
	}
	co := &diskCache[V]{
		basePath:      basePath,
		diskCache:     diskCacheInstance,
		maxExpireTime: maxExpireTime,
	}
	// 启动定时清理任务
	co.autoCleanExpiredFiles()
	return co
}

func (co *diskCache[V]) autoCleanExpiredFiles() {
	if _, ok := basePathMap[co.basePath]; ok {
		return
	}
	defer func() {
		basePathMap[co.basePath] = true
	}()
	goroutines.GoAsync(func(params ...any) {
		cot := params[0].(*diskCache[V])
		for {
			fmt.Printf("\n--- 定时清理触发 (%s) ---\n", conv.String(time.Now()))
			if err := cot.cleanExpiredFiles(co.basePath, co.maxExpireTime); err != nil {
				fmt.Printf("清理错误: %v\n", err)
			}
			time.Sleep(checkInterval)
		}
	}, co)
}

// 清理过期文件（包括子目录）
func (co *diskCache[V]) cleanExpiredFiles(rootDir string, maxAge time.Duration) error {
	//expiryTime := time.Now().Add(-maxAge)
	deletedCount := 0

	// 递归遍历所有文件和子目录
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("访问路径失败: %s, 错误: %w", path, err)
		}
		// 如果是目录，先不处理（等处理完子文件再判断是否删除空目录）
		if info.IsDir() || path == rootDir {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			err = os.Remove(path)
			if err != nil {
				fmt.Printf("删除文件失败1: %s, 错误: %v \n", path, err)
			}
			return nil
		}
		var dataWithExpiryRead DataWithExpiry
		err = conv.Unmarshal(string(content), &dataWithExpiryRead)
		if err != nil {
			err = os.Remove(path)
			if err != nil {
				fmt.Printf("删除文件失败2: %s, 错误: %v \n", path, err)
			}
			return nil
		}

		if time.Now().After(dataWithExpiryRead.Expiry) {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("删除文件失败: %s, 错误: %w", path, err)
			}
			deletedCount++
			fmt.Printf("已删除过期文件: %s (修改时间: %s)\n",
				path, info.ModTime().Format("2006-01-02 15:04:05"))
			return nil
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
func (co *diskCache[V]) Get(_ context.Context, key string) (V, error) {
	ret, ok := co.diskCache.Get(key)
	var zero V
	if !ok {
		return zero, nil
	}
	var dataWithExpiryRead DataWithExpiry
	err := conv.Unmarshal(ret, &dataWithExpiryRead)
	if err != nil {
		return zero, err
	}
	// 判断是否过期
	if time.Now().After(dataWithExpiryRead.Expiry) {
		co.diskCache.Delete(key)
		return zero, nil
	}
	return strToVal[V](dataWithExpiryRead.Data)
}

// Set timeout无效
func (co *diskCache[V]) Set(_ context.Context, key string, val V, timeout time.Duration) (bool, error) {
	dataWithExpiry := DataWithExpiry{
		Data:   conv.String(val),
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

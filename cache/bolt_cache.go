package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
	"time"
)

// boltStoredValue 存储在 bbolt 中的值，含过期时间
type boltStoredValue struct {
	Data      string `json:"d"`
	ExpiresAt int64  `json:"e"` // 过期时间(unix nano), 0 永不过期
}

// BBoltCache 基于 BoltDB 的缓存实现
type BBoltCache[V any] struct {
	db      *bolt.DB
	config  *BBoltCacheConfig
	closeCh chan struct{}
}

// BBoltCacheConfig BoltDB 缓存配置
type BBoltCacheConfig struct {
	DbPath          string        // 数据库文件路径
	TableNameList   []string      // bucket 名称列表
	Namespace       string        // 命名空间，作为 key 前缀隔离不同业务
	RefreshDuration time.Duration // 过期清理间隔，<=0 不启动后台清理
	ErrNotFound     error         // key 不存在时返回的自定义错误
}

// NewBBoltCache 新建基于 BoltDB 的缓存实例
func NewBBoltCache[V any](jConfig *BBoltCacheConfig) (CommCache[V], error) {
	if jConfig == nil {
		jConfig = &BBoltCacheConfig{}
	}
	if jConfig.DbPath == "" {
		return nil, fmt.Errorf("dbPath is empty")
	}
	if jConfig.Namespace == "" {
		jConfig.Namespace = "default"
	}
	if len(jConfig.TableNameList) == 0 {
		jConfig.TableNameList = []string{jConfig.Namespace}
	}
	jConfig.TableNameList = lo.Uniq(jConfig.TableNameList)

	// 打开 BoltDB 数据库
	db, err := bolt.Open(jConfig.DbPath, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("bolt open failed: %w", err)
	}

	// 创建所有 bucket（使用 CreateBucketIfNotExists 确保幂等）
	err = db.Update(func(tx *bolt.Tx) error {
		for _, tableName := range jConfig.TableNameList {
			if _, err := tx.CreateBucketIfNotExists([]byte(tableName)); err != nil {
				return fmt.Errorf("create bucket %s failed: %w", tableName, err)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	cache := &BBoltCache[V]{
		db:      db,
		config:  jConfig,
		closeCh: make(chan struct{}),
	}

	// 启动后台过期清理
	if jConfig.RefreshDuration > 0 {
		go cache.cleanExpiredLoop()
	}

	return cache, nil
}

// buildKey 构建带命名空间前缀的存储 key
func (co *BBoltCache[V]) buildKey(key string) string {
	return getNsKey(co.config.Namespace, key)
}

// getDefaultBucket 获取默认 bucket 名称
func (co *BBoltCache[V]) getDefaultBucket() string {
	return co.config.TableNameList[0]
}

// isClosed 判断数据库是否已关闭
func (co *BBoltCache[V]) isClosed() bool {
	select {
	case <-co.closeCh:
		return true
	default:
		return false
	}
}

var errDBClosed = fmt.Errorf("bolt database is closed")

// Get 从缓存中取得一个值
func (co *BBoltCache[V]) Get(ctx context.Context, key string) (v V, err error) {
	if co.isClosed() {
		return v, errDBClosed
	}
	bucketName := co.getDefaultBucket()
	storeKey := co.buildKey(key)

	err = co.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			if co.config.ErrNotFound != nil {
				return co.config.ErrNotFound
			}
			return fmt.Errorf("bucket %s not found", bucketName)
		}

		data := b.Get([]byte(storeKey))
		if data == nil {
			if co.config.ErrNotFound != nil {
				return co.config.ErrNotFound
			}
			return nil
		}

		var stored boltStoredValue
		if err = conv.Unmarshal(string(data), &stored); err != nil {
			return fmt.Errorf("unmarshal stored value failed: %w", err)
		}

		// 检查是否过期
		if stored.ExpiresAt > 0 && time.Now().UnixNano() > stored.ExpiresAt {
			if co.config.ErrNotFound != nil {
				return co.config.ErrNotFound
			}
			return nil
		}

		v, err = strToVal[V](stored.Data)
		return err
	})

	return v, err
}

// Set 设置缓存值，timeout 指定过期时间
func (co *BBoltCache[V]) Set(ctx context.Context, key string, val V, timeout time.Duration) (bool, error) {
	if co.isClosed() {
		return false, errDBClosed
	}
	bucketName := co.getDefaultBucket()
	storeKey := co.buildKey(key)

	var expiresAt int64
	if timeout > 0 {
		expiresAt = time.Now().Add(timeout).UnixNano()
	}

	stored := boltStoredValue{
		Data:      conv.String(val),
		ExpiresAt: expiresAt,
	}

	data := conv.String(stored)
	err := co.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucketName)
		}
		return b.Put([]byte(storeKey), []byte(data))
	})

	if err != nil {
		return false, err
	}
	return true, nil
}

// Del 从缓存中删除一个 key
func (co *BBoltCache[V]) Del(ctx context.Context, key string) (bool, error) {
	if co.isClosed() {
		return false, errDBClosed
	}
	bucketName := co.getDefaultBucket()
	storeKey := co.buildKey(key)

	err := co.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucketName)
		}
		return b.Delete([]byte(storeKey))
	})

	if err != nil {
		return false, err
	}
	return true, nil
}

// cleanExpiredLoop 后台定期清理过期数据
func (co *BBoltCache[V]) cleanExpiredLoop() {
	ticker := time.NewTicker(co.config.RefreshDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			co.cleanExpired()
		case <-co.closeCh:
			return
		}
	}
}

// cleanExpired 清理当前 bucket 中的过期数据
func (co *BBoltCache[V]) cleanExpired() {
	if co.isClosed() {
		return
	}
	now := time.Now().UnixNano()
	bucketName := co.getDefaultBucket()

	_ = co.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}

		c := b.Cursor()
		var toDelete [][]byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var stored boltStoredValue
			if err := json.Unmarshal(v, &stored); err != nil {
				continue
			}
			if stored.ExpiresAt > 0 && stored.ExpiresAt <= now {
				toDelete = append(toDelete, k)
			}
		}

		for _, k := range toDelete {
			_ = b.Delete(k)
		}
		return nil
	})
}

// Close 关闭数据库连接，停止后台清理协程
func (co *BBoltCache[V]) Close() error {
	select {
	case <-co.closeCh:
		// 已关闭
	default:
		close(co.closeCh)
	}
	return co.db.Close()
}

// DB 获取底层 BoltDB 实例，供高级操作使用
func (co *BBoltCache[V]) DB() (*bolt.DB, error) {
	if co.db == nil {
		return nil, fmt.Errorf("database is closed")
	}
	if co.isClosed() {
		return nil, errDBClosed
	}
	return co.db, nil
}

// Update 执行一个写事务，类似 MySQL 的 UPDATE / 任意 DML。
// fn 中可通过 tx.Bucket([]byte(tableName)) 获取 bucket 进行读写操作。
func (co *BBoltCache[V]) Update(tableName string, key string, fn func(v V) (V, error)) error {
	if co.isClosed() {
		return errDBClosed
	}
	return co.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return fmt.Errorf("create table %s failed: %w", tableName, err)
		}
		v, err := co.FindOne(tableName, key)
		if err != nil {
			return err
		}
		v, err = fn(v)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(conv.String(v)))
	})
}

// Insert 向指定表中插入一条记录，类似 MySQL 的 INSERT。
// key 必须唯一，若已存在则返回错误。
func (co *BBoltCache[V]) Insert(tableName string, key string, one V) error {
	if co.isClosed() {
		return errDBClosed
	}
	data := conv.String(one)
	return co.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return fmt.Errorf("create table %s failed: %w", tableName, err)
		}
		if existing := b.Get([]byte(key)); existing != nil {
			return fmt.Errorf("duplicate key %s in table %s", key, tableName)
		}
		return b.Put([]byte(key), []byte(data))
	})
}

// InsertList 批量插入记录，使用自增 ID 作为 key，类似 MySQL 的 auto_increment 批量 INSERT。
func (co *BBoltCache[V]) InsertList(tableName string, listMap map[string]V) error {
	if co.isClosed() {
		return errDBClosed
	}
	if len(listMap) == 0 {
		return nil
	}

	return co.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return fmt.Errorf("create table %s failed: %w", tableName, err)
		}

		var retErr error

		for key, one := range listMap {
			if existing := b.Get([]byte(key)); existing != nil {
				retErr = multierror.Append(retErr, fmt.Errorf("duplicate key %s in table %s", key, tableName))
				continue
			}
			data := conv.String(one)
			if err = b.Put([]byte(key), []byte(data)); err != nil {
				retErr = multierror.Append(retErr, fmt.Errorf("insert key %s failed: %w", key, err))
				continue
			}
		}
		if retErr != nil {
			return retErr
		}
		return nil
	})
}

// Select 执行一个读事务，类似 MySQL 的 SELECT。
// fn 中可通过 tx.Bucket([]byte(tableName)) 获取 bucket 进行只读查询。
func (co *BBoltCache[V]) Select(tableName string, fn func(tx *bolt.Tx, tableInfo *bolt.Bucket) error) error {
	if co.isClosed() {
		return errDBClosed
	}
	return co.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table %s not found", tableName)
		}
		return fn(tx, b)
	})
}

// FindOne 根据 key 在指定表中查找一条记录，类似 MySQL 的 SELECT ... WHERE key=? LIMIT 1。
func (co *BBoltCache[V]) FindOne(tableName string, key string) (v V, err error) {
	if co.isClosed() {
		return v, errDBClosed
	}

	err = co.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			if co.config.ErrNotFound != nil {
				return co.config.ErrNotFound
			}
			return fmt.Errorf("table %s not found", tableName)
		}

		data := b.Get([]byte(key))
		if data == nil {
			if co.config.ErrNotFound != nil {
				return co.config.ErrNotFound
			}
			return fmt.Errorf("record not found: key=%s in table %s", key, tableName)
		}
		v, err = strToVal[V](string(data))
		return err
	})

	return v, err
}

//写入数据
//
//err := db.Update(func(tx *bolt.Tx) error {
//	bucket := tx.Bucket([]byte("Users"))
//	if bucket == nil {
//		return errors.New("bucket not found")
//	}
//
//	// 存储键值对
//	err := bucket.Put([]byte("user:1"), []byte("Alice"))
//	return err
//})
//
//读取数据
//
//err := db.View(func(tx *bolt.Tx) error {
//	bucket := tx.Bucket([]byte("Users"))
//	if bucket == nil {
//		return errors.New("bucket not found")
//	}
//
//	// 获取值
//	val := bucket.Get([]byte("user:1"))
//	fmt.Printf("用户: %s\n", val)
//	return nil
//})
//
//
//
//核心概念
//
//Bucket（桶）
//
//Bucket 是键值对的集合，相当于关系数据库中的"表"。
//
//所有键在一个 Bucket 内必须唯一
//
//支持 Bucket 嵌套（多层结构）
//
//每个 Bucket 可以有自己的配置
//
//Transaction（事务）
//
//bbolt 支持两种事务：
//
//读写事务（Update）
//
//err := db.Update(func(tx *bolt.Tx) error {
//	// 可以读写
//	return nil
//})
//
//同一时间只能有一个读写事务
//
//适合写入、修改、删除操作
//
//只读事务（View）
//
//err := db.View(func(tx *bolt.Tx) error {
//	// 只能读取
//	return nil
//})
//
//可以有多个并发只读事务
//
//适合查询操作
//
//Cursor（游标）
//
//用于遍历 Bucket 中的键值对：
//
//err := db.View(func(tx *bolt.Tx) error {
//	bucket := tx.Bucket([]byte("Users"))
//	c := bucket.Cursor()
//
//	// 从第一个开始遍历
//	for k, v := c.First(); k != nil; k, v = c.Next() {
//		fmt.Printf("key=%s, value=%s\n", k, v)
//	}
//
//	return nil
//})
//
//
//
//进阶用法
//
//自增 ID
//
//bbolt 提供了 NextSequence() 生成自增 ID：
//
//type User struct {
//	ID   int
//	Name string
//}
//
//func CreateUser(db *bolt.DB, name string) (int, error) {
//	var userID int
//
//	err := db.Update(func(tx *bolt.Tx) error {
//		bucket := tx.Bucket([]byte("Users"))
//
//		// 生成自增 ID
//		id, _ := bucket.NextSequence()
//		userID = int(id)
//
//		user := User{ID: userID, Name: name}
//		data, _ := json.Marshal(user)
//
//		// 以 ID 为键存储
//		return bucket.Put(itob(userID), data)
//	})
//
//	return userID, err
//}
//
//// int 转 []byte
//func itob(v int) []byte {
//	b := make([]byte, 8)
//	binary.BigEndian.PutUint64(b, uint64(v))
//	return b
//}
//
//范围查询
//
//支持按前缀或范围查询：
//
//// 前缀查询
//err := db.View(func(tx *bolt.Tx) error {
//	c := tx.Bucket([]byte("Events")).Cursor()
//
//	prefix := []byte("user:")
//	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
//		fmt.Printf("key=%s, value=%s\n", k, v)
//	}
//	return nil
//})
//
//// 范围查询（时间范围）
//err := db.View(func(tx *bolt.Tx) error {
//	c := tx.Bucket([]byte("Events")).Cursor()
//
//	min := []byte("2024-01-01")
//	max := []byte("2024-12-31")
//
//	for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
//		fmt.Printf("%s: %s\n", k, v)
//	}
//	return nil
//})
//
//批量写入
//
//对于大量写入操作，使用 Batch 提升性能：
//
//err := db.Batch(func(tx *bolt.Tx) error {
//	bucket := tx.Bucket([]byte("Users"))
//
//	for i := 0; i < 1000; i++ {
//		key := []byte(fmt.Sprintf("user:%d", i))
//		bucket.Put(key, []byte("data"))
//	}
//
//	return nil
//})
//
//Batch 会自动合并多个事务，减少磁盘写入次数。
//
//嵌套 Bucket
//
//支持多层结构：
//
//err := db.Update(func(tx *bolt.Tx) error {
//	// 创建用户 bucket
//	users := tx.Bucket([]byte("Users"))
//
//	// 为用户创建子 bucket
//	user1, _ := users.CreateBucketIfNotExists([]byte("user:1"))
//
//	// 在子 bucket 中存储数据
//	user1.Put([]byte("profile"), []byte("Alice's profile"))
//	user1.Put([]byte("settings"), []byte("{}"))
//
//	return nil
//})
//
//数据库备份
//
//由于是单文件，备份非常简单：
//
//// 方式一：直接复制文件
//func Backup() {
//	db.View(func(tx *bolt.Tx) error {
//		tx.WriteTo(file)
//		return nil
//	})
//}
//
//// 方式二：HTTP 备份
//func BackupHandler(w http.ResponseWriter, r *http.Request) {
//	db.View(func(tx *bolt.Tx) error {
//		w.Header().Set("Content-Type", "application/octet-stream")
//		w.Header().Set("Content-Disposition", "attachment; filename=backup.db")
//		tx.WriteTo(w)
//		return nil
//	})
//}

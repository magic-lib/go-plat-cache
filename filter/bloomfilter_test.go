package filter_test

import (
	"context"
	"fmt"
	"github.com/hugh2632/bloomfilter"
	"github.com/hugh2632/bloomfilter/memory"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"hash"
	"hash/crc64"
	"strconv"
	"testing"
)

func fillNums(filter bloomfilter.IFilter, begin int, end int) {
	for i := begin; i < end+1; i++ {
		filter.Push([]byte(strconv.Itoa(i)))
	}
	fmt.Printf("已填入%d-%d的数据\n", begin, end)
}

var options = &redis.Options{
	Addr:     "192.168.20.101:6379",
	Username: "",
	Password: "",
	DB:       0,
}

var key = "test"

func TestRedisCachedFilter(t *testing.T) {
	cli := redis.NewClient(options)
	cachedFilter, err := bloomfilter.NewRedisFilter(context.TODO(), cli, bloomfilter.RedisFilterType_Cached, key, 10240, bloomfilter.DefaultHash...)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("误判率，False positive rate:", bloomfilter.GetFlasePositiveRate(10240*8, 3, 2)) // 5.364385288193686e-11
	fillNums(cachedFilter, 250, 300)
	t.Log(cachedFilter.Exists([]byte(strconv.Itoa(290)))) // true
	t.Log(cachedFilter.Exists([]byte(strconv.Itoa(299)))) // true
	t.Log(cachedFilter.Exists([]byte(strconv.Itoa(350)))) // false
	// must use write to save the data to redis.
	// 必须使用write方法将数据全部提交到redis服务器
	cachedFilter.Write()
}

var sqlDSN = "user:password@tcp(ip:port)/database?charset=utf8mb4&parseTime=True&loc=Local"

func TestSqlFilter(t *testing.T) {
	// Init gorm.DB
	db, err := gorm.Open(mysql.Open(sqlDSN))
	if err != nil {
		t.Fatal(err)
	}
	// Init SQLFilter
	sqlFilter, err := bloomfilter.SqlFilter(db, "test", 1000, bloomfilter.DefaultHash...)
	if err != nil {
		t.Fatal(err)
	}
	// Push 250-300 numbers to the filter.
	// 把250-300的数字压入过滤器
	fillNums(sqlFilter, 250, 300)
	sqlFilter.Write()
}

func TestSqlFilterExist(t *testing.T) {
	db, err := gorm.Open(mysql.Open(sqlDSN))
	if err != nil {
		t.Fatal(err)
	}
	sqlFilter, err := bloomfilter.SqlFilter(db, "test", 1000, bloomfilter.DefaultHash...)
	if err != nil {
		t.Fatal(err)
	}
	// 280-300 should exist in filter, and 301-320 doesn't.
	// 280-300应该在过滤器中，而301-320不应该在。
	for i := 280; i < 320; i++ {
		t.Logf("%d: %t", i, sqlFilter.Exists([]byte(strconv.Itoa(i))))
	}
}

func TestInteractiveFilter(t *testing.T) {
	cli := redis.NewClient(options)
	interactiveFilter, err := bloomfilter.NewRedisFilter(context.TODO(), cli, bloomfilter.RedisFilterType_Interactive, key, 10240, bloomfilter.DefaultHash...)
	if err != nil {
		t.Fatal(err)
	}
	fillNums(interactiveFilter, 250, 300)

	anotherFilter, _ := bloomfilter.NewRedisFilter(context.TODO(), cli, bloomfilter.RedisFilterType_Interactive, key, 10240, bloomfilter.DefaultHash...)

	t.Log(interactiveFilter.Exists([]byte(strconv.Itoa(290)))) // true
	t.Log(anotherFilter.Exists([]byte(strconv.Itoa(290))))     // true

	t.Log(interactiveFilter.Exists([]byte(strconv.Itoa(299)))) // true
	t.Log(anotherFilter.Exists([]byte(strconv.Itoa(299))))     // true

	t.Log(interactiveFilter.Exists([]byte(strconv.Itoa(320)))) // false
	t.Log(anotherFilter.Exists([]byte(strconv.Itoa(320))))     // false
}

func TestMemFalsePositiveRate(t *testing.T) {
	memFilter := bloomfilter.NewMemoryFilter(make([]byte, 10240), bloomfilter.DefaultHash...).(*memory.Filter)
	testFalsePositiveRate(t, memFilter, 10240*8, 1000, 3, 100000000)
	//=== RUN   TestMemFalsePositiveRate
	//    common.go:37: 理论误判率 Theoretical false positive rate: 0.0018230817954481005
	//    common.go:49: 实际误判率 Real false positive rate:0.00047424474244742447
	//--- PASS: TestMemFalsePositiveRate (7.43s)
	//PASS
}

func TestIsHashFuncUniformlyDistributed(t *testing.T) {
	for _, f := range bloomfilter.DefaultHash {
		t.Log(bloomfilter.CalculateInformationEntropy(f))
	}
	//不满足的hash，已舍弃
	t.Log("abandoned hash function:")
	t.Log(bloomfilter.CalculateInformationEntropy(func() hash.Hash64 { return crc64.New(crc64.MakeTable(crc64.ISO)) }))
}

package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"time"
)

const bucketHeight = 8

type fingerprint uint16

type target struct {
	bucketIndex uint
	fingerprint fingerprint
}

type bucket struct {
	entries [bucketHeight]fingerprint
	count   uint8
}

type table []bucket

type countingFilter[V bool] struct {
	tables     []table
	numTables  uint
	numBuckets uint
}

func newCountingFilter(numTables uint, numBuckets uint) (*countingFilter[bool], error) {
	if numBuckets < numTables {
		return nil, errors.New("numBuckets has to be greater than numTables")
	}

	cf := &countingFilter[bool]{
		numTables:  numTables,
		numBuckets: numBuckets,
		tables:     make([]table, numTables),
	}

	for i := range cf.tables {
		cf.tables[i] = make(table, numBuckets)
	}

	return cf, nil
}

func NewCountingFilter(capacity int) (CommCache[bool], error) {
	if capacity <= 0 {
		capacity = defaultFilterSize
	}
	t := capacity / (4096 * bucketHeight)
	return newCountingFilter(uint(t), 4096)
}

// Get 从缓存中获取值
func (cf *countingFilter[V]) Get(_ context.Context, key string) (bool, error) {
	targets := cf.getTargets([]byte(key))
	_, _, bfp := cf.lookup(targets)
	return bfp != nil, nil
}

// Set 向缓存中设置值
func (cf *countingFilter[V]) Set(_ context.Context, key string, _ V, _ time.Duration) (bool, error) {
	return cf.add([]byte(key)), nil
}

// Del 从缓存中删除键
func (cf *countingFilter[V]) Del(_ context.Context, key string) (bool, error) {
	return cf.delete([]byte(key)), nil
}

func (cf *countingFilter[V]) getTargets(data []byte) []target {
	hashMethod := fnv.New64a()
	_, _ = hashMethod.Write(data)
	fp := hashMethod.Sum(nil)
	hashSum := hashMethod.Sum64()

	h1 := uint32(hashSum & 0xffffffff)
	h2 := uint32((hashSum >> 32) & 0xffffffff)

	indices := make([]uint, cf.numTables)
	for i := uint(0); i < cf.numTables; i++ {
		saltedHash := uint(h1 + uint32(i)*h2)
		indices[i] = saltedHash % cf.numBuckets
	}

	targets := make([]target, cf.numTables)
	for i := uint(0); i < cf.numTables; i++ {
		targets[i] = target{
			bucketIndex: uint(indices[i]),
			fingerprint: fingerprint(binary.LittleEndian.Uint16(fp)),
		}
	}
	return targets
}

func (cf *countingFilter[V]) add(data []byte) bool {
	targets := cf.getTargets(data)

	_, _, target := cf.lookup(targets)
	if target != nil {
		return false
	}

	minCount := uint8(math.MaxUint8)
	tableI := uint(0)

	for i, target := range targets {
		tmpCount := cf.tables[i][target.bucketIndex].count
		if tmpCount < minCount && tmpCount < bucketHeight {
			minCount = cf.tables[i][target.bucketIndex].count
			tableI = uint(i)
		}
	}

	if minCount == uint8(math.MaxUint8) {
		return false
	}
	bucket := &cf.tables[tableI][targets[tableI].bucketIndex]
	bucket.entries[minCount] = targets[tableI].fingerprint
	bucket.count++
	return true
}

func (cf *countingFilter[V]) delete(data []byte) bool {
	deleted := false
	targets := cf.getTargets(data)
	for i, target := range targets {
		for j, fp := range cf.tables[i][target.bucketIndex].entries {
			if fp == target.fingerprint {
				if cf.tables[i][target.bucketIndex].count == 0 {
					continue
				}
				cf.tables[i][target.bucketIndex].count--
				k := 0
				for l, fp := range cf.tables[i][target.bucketIndex].entries {
					if j == l {
						continue
					}
					cf.tables[i][target.bucketIndex].entries[k] = fp
					k++
				}
				lastIndex := cf.tables[i][target.bucketIndex].count
				cf.tables[i][target.bucketIndex].entries[lastIndex] = 0
				deleted = true
			}
		}
	}
	return deleted
}

func (cf *countingFilter[V]) lookup(targets []target) (uint, uint, *target) {
	for i, target := range targets {
		for j, fp := range cf.tables[i][target.bucketIndex].entries {
			if fp == target.fingerprint {
				return uint(i), uint(j), &target
			}
		}
	}
	return 0, 0, nil
}

func (cf *countingFilter[V]) GetCount() uint {
	count := uint(0)
	for _, table := range cf.tables {
		for _, bucket := range table {
			count += uint(bucket.count)
		}
	}
	return count
}

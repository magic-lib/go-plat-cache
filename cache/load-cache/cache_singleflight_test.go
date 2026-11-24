package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapCache 测试用的内存缓存，实现 CommCache
type mapCache[V any] struct {
	mu     sync.Mutex
	data   map[string]V
	setErr error
}

func newMapCache[V any]() *mapCache[V] {
	return &mapCache[V]{data: make(map[string]V)}
}

func (m *mapCache[V]) Get(_ context.Context, key string) (V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key], nil
}

func (m *mapCache[V]) Set(_ context.Context, key string, val V, _ time.Duration) (bool, error) {
	if m.setErr != nil {
		return false, m.setErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val
	return true, nil
}

func (m *mapCache[V]) Del(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return true, nil
}

func (m *mapCache[V]) value(key string) V {
	val, _ := m.Get(context.Background(), key)
	return val
}

// runConcurrent 同时发起 n 个请求，返回每个请求的结果
func runConcurrent[V any](n int, do func() (V, error)) ([]V, []error) {
	vals := make([]V, n)
	errs := make([]error, n)
	var start, done sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			vals[i], errs[i] = do()
		}(i)
	}
	start.Done()
	done.Wait()
	return vals, errs
}

// 未设置缓存：并发请求只执行一次，共享同一个结果
func TestSingleFlightNoCache(t *testing.T) {
	sf := NewSingleFlight[int]()
	var count int32
	vals, errs := runConcurrent(50, func() (int, error) {
		return sf.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
			atomic.AddInt32(&count, 1)
			time.Sleep(100 * time.Millisecond)
			return 42, nil
		})
	})
	if count != 1 {
		t.Fatalf("fn executed %d times, want 1", count)
	}
	for i := range vals {
		if errs[i] != nil || vals[i] != 42 {
			t.Fatalf("request %d got %v %v", i, vals[i], errs[i])
		}
	}
}

// 未设置缓存：执行完以后，下一批请求会重新执行
func TestSingleFlightNoCacheNextRound(t *testing.T) {
	sf := NewSingleFlight[int]()
	var count int32
	fn := func(ctx context.Context) (int, error) {
		return int(atomic.AddInt32(&count, 1)), nil
	}
	first, _ := sf.Do(context.Background(), "k", fn)
	second, _ := sf.Do(context.Background(), "k", fn)
	if first != 1 || second != 2 {
		t.Fatalf("got %d %d, want 1 2", first, second)
	}
}

// 设置缓存但未命中：只执行一次，结果写入缓存
func TestSingleFlightCacheMiss(t *testing.T) {
	c := newMapCache[string]()
	sf := NewSingleFlight(WithCache[string](c, time.Minute))
	var count int32
	vals, errs := runConcurrent(50, func() (string, error) {
		return sf.Do(context.Background(), "k", func(ctx context.Context) (string, error) {
			atomic.AddInt32(&count, 1)
			time.Sleep(100 * time.Millisecond)
			return "v1", nil
		})
	})
	if count != 1 {
		t.Fatalf("fn executed %d times, want 1", count)
	}
	for i := range vals {
		if errs[i] != nil || vals[i] != "v1" {
			t.Fatalf("request %d got %v %v", i, vals[i], errs[i])
		}
	}
	if got := c.value("k"); got != "v1" {
		t.Fatalf("cache value = %q, want v1", got)
	}
}

// 设置缓存且命中：所有请求立即拿到旧值，后台只执行一次刷新并写回缓存
func TestSingleFlightCacheHitRefresh(t *testing.T) {
	c := newMapCache[string]()
	_, _ = c.Set(context.Background(), "k", "old", time.Minute)
	sf := NewSingleFlight(WithCache[string](c, time.Minute))

	release := make(chan struct{})
	var count int32
	fn := func(ctx context.Context) (string, error) {
		atomic.AddInt32(&count, 1)
		<-release // 刷新被阻塞时，请求仍应立即返回旧值
		return "new", nil
	}
	vals, errs := runConcurrent(50, func() (string, error) {
		return sf.Do(context.Background(), "k", fn)
	})
	for i := range vals {
		if errs[i] != nil || vals[i] != "old" {
			t.Fatalf("request %d got %v %v, want old", i, vals[i], errs[i])
		}
	}
	close(release)
	waitFor(t, func() bool { return c.value("k") == "new" })
	if count != 1 {
		t.Fatalf("refresh executed %d times, want 1", count)
	}
}

// 后台刷新不受请求 ctx 取消的影响
func TestSingleFlightRefreshIgnoresCancel(t *testing.T) {
	c := newMapCache[string]()
	_, _ = c.Set(context.Background(), "k", "old", time.Minute)
	sf := NewSingleFlight(WithCache[string](c, time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	val, err := sf.Do(ctx, "k", func(ctx context.Context) (string, error) {
		time.Sleep(50 * time.Millisecond)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "new", nil
	})
	cancel()
	if err != nil || val != "old" {
		t.Fatalf("got %v %v", val, err)
	}
	waitFor(t, func() bool { return c.value("k") == "new" })
}

// 后台刷新出错或 panic：交给 errorHandler，缓存保留旧值，进程不崩溃
func TestSingleFlightRefreshErrorAndPanic(t *testing.T) {
	c := newMapCache[string]()
	_, _ = c.Set(context.Background(), "k", "old", time.Minute)
	var mu sync.Mutex
	var errs []error
	sf := NewSingleFlight(WithCache[string](c, time.Minute), WithErrorHandler[string](func(key string, err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}))
	gotErrs := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(errs)
	}

	val, err := sf.Do(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "", errors.New("load failed")
	})
	if err != nil || val != "old" {
		t.Fatalf("got %v %v", val, err)
	}
	waitFor(t, func() bool { return gotErrs() == 1 })

	val, err = sf.Do(context.Background(), "k", func(ctx context.Context) (string, error) {
		panic("boom")
	})
	if err != nil || val != "old" {
		t.Fatalf("got %v %v", val, err)
	}
	waitFor(t, func() bool { return gotErrs() == 2 })
	if c.value("k") != "old" {
		t.Fatalf("cache value changed to %q", c.value("k"))
	}
}

// 未命中时 fn 出错：错误返回给所有等待的请求，不写缓存
func TestSingleFlightLoadError(t *testing.T) {
	c := newMapCache[string]()
	sf := NewSingleFlight(WithCache[string](c, time.Minute))
	loadErr := errors.New("load failed")
	_, errs := runConcurrent(10, func() (string, error) {
		return sf.Do(context.Background(), "k", func(ctx context.Context) (string, error) {
			time.Sleep(50 * time.Millisecond)
			return "", loadErr
		})
	})
	for i, err := range errs {
		if !errors.Is(err, loadErr) {
			t.Fatalf("request %d err = %v, want load failed", i, err)
		}
	}
	if c.value("k") != "" {
		t.Fatalf("cache should stay empty, got %q", c.value("k"))
	}
}

// 写缓存失败：结果照常返回，错误交给 errorHandler
func TestSingleFlightSetCacheError(t *testing.T) {
	c := newMapCache[string]()
	c.setErr = errors.New("cache down")
	var handled error
	sf := NewSingleFlight(WithCache[string](c, time.Minute), WithErrorHandler[string](func(key string, err error) {
		handled = err
	}))
	val, err := sf.Do(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "v1", nil
	})
	if err != nil || val != "v1" {
		t.Fatalf("got %v %v", val, err)
	}
	if !errors.Is(handled, c.setErr) {
		t.Fatalf("handled error = %v, want cache down", handled)
	}
}

// 缓存里是零值视为未命中，会同步执行 fn
func TestSingleFlightZeroValueIsMiss(t *testing.T) {
	c := newMapCache[int]()
	_, _ = c.Set(context.Background(), "k", 0, time.Minute)
	sf := NewSingleFlight(WithCache[int](c, time.Minute))
	val, err := sf.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
		return 7, nil
	})
	if err != nil || val != 7 {
		t.Fatalf("got %v %v, want 7", val, err)
	}
}

func TestSingleFlightNilFn(t *testing.T) {
	if _, err := NewSingleFlight[int]().Do(context.Background(), "k", nil); err == nil {
		t.Fatal("expected error for nil fn")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

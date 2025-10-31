package cache

//c := cache.NewCache(cache.CacheOptions{
//DefaultTTL:    10 * time.Second,
//EmptyTTL:      2 * time.Second,
//RefreshFactor: 0.3,
//ShardCount:    8,
//TotalMaxBytes: 1 << 20,
//})
//
//// 带 context 的加载器
//loader := func(ctx context.Context) (interface{}, time.Duration, error) {
//	// 从 DB / 网络加载
//	return "value", 5 * time.Second, nil
//}
//
//val, err := c.GetOrLoadCtx(context.Background(), "key-1", loader)

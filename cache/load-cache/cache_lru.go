package cache

// weightOf 计算缓存值的权重（如字节数），用于 LRU 驱逐。
func weightOf(val interface{}) int64 {
	switch v := val.(type) {
	case string:
		return int64(len(v))
	default:
		return 1
	}
}

package cache

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 用于 Prometheus 监控缓存命中、丢失、驱逐等指标。
type Metrics struct {
	Hits      prometheus.Counter // 命中次数
	Misses    prometheus.Counter // 丢失次数
	Evictions prometheus.Counter // 驱逐次数
	Refreshes prometheus.Counter // 刷新次数
}

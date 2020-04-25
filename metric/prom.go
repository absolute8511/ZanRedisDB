package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ClusterWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cluster_write_latency",
		Help:    "cluster write request latency",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace"})
	DBWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db_write_latency",
		Help:    "db write request latency",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace"})
	RaftWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "raft_write_latency",
		Help:    "raft write request latency",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace", "step"})

	WriteByteSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "write_byte_size",
		Help:    "write request byte size",
		Buckets: prometheus.ExponentialBuckets(128, 2, 12),
	}, []string{"namespace"})

	SlowWrite100msCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slow_write_100ms_cnt",
		Help: "slow 100ms counter for slow write command",
	}, []string{"table", "cmd"})
	SlowWrite50msCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slow_write_50ms_cnt",
		Help: "slow 50ms counter for slow write command",
	}, []string{"table", "cmd"})
	SlowWrite10msCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slow_write_10ms_cnt",
		Help: "slow 10ms counter for slow write command",
	}, []string{"table", "cmd"})
	SlowLimiterRefusedCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slow_limiter_refused_cnt",
		Help: "slow limiter refused counter for slow write command",
	}, []string{"table", "cmd"})
)

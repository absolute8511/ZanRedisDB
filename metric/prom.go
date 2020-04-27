package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// unit is ms
	ClusterWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cluster_write_latency",
		Help:    "cluster write request latency",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace"})
	// unit is ms
	DBWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db_write_latency",
		Help:    "db write request latency",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace"})
	// unit is ms
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

	QueueLen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_len",
		Help: "queue length for all kinds of queue like raft proposal/transport/apply",
	}, []string{"namespace", "queue_name"})

	ErrorCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "error_cnt",
		Help: "error counter for some useful kinds of internal error",
	}, []string{"namespace", "error_info"})

	EventCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "event_cnt",
		Help: "the important event counter for internal event",
	}, []string{"namespace", "event_name"})
)

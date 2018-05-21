package common

import (
	"math"
	"sync/atomic"
)

type WriteStats struct {
	// <100bytes, <1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB, 2MB, 4MB
	ValueSizeStats [16]int64 `json:"value_size_stats"`
	// <1024us, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s
	WriteLatencyStats [16]int64 `json:"write_latency_stats"`
}

func (ws *WriteStats) UpdateSizeStats(vSize int64) {
	bucket := 0
	if vSize < 100 {
	} else if vSize < 1024 {
		bucket = 1
	} else if vSize >= 1024 {
		bucket = int(math.Log2(float64(vSize/1024))) + 2
	}
	if bucket >= len(ws.ValueSizeStats) {
		bucket = len(ws.ValueSizeStats) - 1
	}
	atomic.AddInt64(&ws.ValueSizeStats[bucket], 1)
}

func (ws *WriteStats) BatchUpdateLatencyStats(latencyUs int64, cnt int64) {
	bucket := 0
	if latencyUs < 1024 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
	}
	if bucket >= len(ws.WriteLatencyStats) {
		bucket = len(ws.WriteLatencyStats) - 1
	}
	atomic.AddInt64(&ws.WriteLatencyStats[bucket], cnt)
}

func (ws *WriteStats) UpdateLatencyStats(latencyUs int64) {
	bucket := 0
	if latencyUs < 1024 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
	}
	if bucket >= len(ws.WriteLatencyStats) {
		bucket = len(ws.WriteLatencyStats) - 1
	}
	atomic.AddInt64(&ws.WriteLatencyStats[bucket], 1)
}

func (ws *WriteStats) UpdateWriteStats(vSize int64, latencyUs int64) {
	ws.UpdateSizeStats(vSize)
	ws.UpdateLatencyStats(latencyUs)
}

func (ws *WriteStats) Copy() *WriteStats {
	var s WriteStats
	for i := 0; i < len(ws.ValueSizeStats); i++ {
		s.ValueSizeStats[i] = atomic.LoadInt64(&ws.ValueSizeStats[i])
	}
	for i := 0; i < len(ws.WriteLatencyStats); i++ {
		s.WriteLatencyStats[i] = atomic.LoadInt64(&ws.WriteLatencyStats[i])
	}
	return &s
}

type TableStats struct {
	Name              string `json:"name"`
	KeyNum            int64  `json:"key_num"`
	DiskBytesUsage    int64  `json:"disk_bytes_usage"`
	ApproximateKeyNum int64  `json:"approximate_key_num"`
}

type NamespaceStats struct {
	Name              string                 `json:"name"`
	TStats            []TableStats           `json:"table_stats"`
	DBWriteStats      *WriteStats            `json:"db_write_stats"`
	ClusterWriteStats *WriteStats            `json:"cluster_write_stats"`
	InternalStats     map[string]interface{} `json:"internal_stats"`
	EngType           string                 `json:"eng_type"`
	IsLeader          bool                   `json:"is_leader"`
}

type LogSyncStats struct {
	Name      string `json:"name"`
	Term      uint64 `json:"term"`
	Index     uint64 `json:"index"`
	Timestamp int64  `json:"timestamp"`
	IsLeader  bool   `json:"is_leader"`
}

type ScanStats struct {
	ScanCount uint64 `json:"scan_count"`
	// <1024us, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s
	ScanLatencyStats [16]int64 `json:"scan_latency_stats"`
}

func (ss *ScanStats) IncScanCount() {
	atomic.AddUint64(&ss.ScanCount, 1)
}

func (ss *ScanStats) UpdateLatencyStats(latencyUs int64) {
	bucket := 0
	if latencyUs < 1024 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
	}
	if bucket >= len(ss.ScanLatencyStats) {
		bucket = len(ss.ScanLatencyStats) - 1
	}
	atomic.AddInt64(&ss.ScanLatencyStats[bucket], 1)
}

func (ss *ScanStats) UpdateScanStats(latencyUs int64) {
	ss.IncScanCount()
	ss.UpdateLatencyStats(latencyUs)
}

func (ss *ScanStats) Copy() *ScanStats {
	var s ScanStats
	s.ScanCount = atomic.LoadUint64(&ss.ScanCount)
	for i := 0; i < len(ss.ScanLatencyStats); i++ {
		s.ScanLatencyStats[i] = atomic.LoadInt64(&ss.ScanLatencyStats[i])
	}
	return &s
}

type ServerStats struct {
	// database stats
	NSStats []NamespaceStats `json:"ns_stats"`
	//scan统计
	ScanStats *ScanStats `json:"scan_stats"`

	// other server related stats
}

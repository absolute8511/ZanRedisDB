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

func (self *WriteStats) UpdateSizeStats(vSize int64) {
	bucket := 0
	if vSize < 100 {
	} else if vSize < 1024 {
		bucket = 1
	} else if vSize >= 1024 {
		bucket = int(math.Log2(float64(vSize/1024))) + 2
	}
	if bucket >= len(self.ValueSizeStats) {
		bucket = len(self.ValueSizeStats) - 1
	}
	atomic.AddInt64(&self.ValueSizeStats[bucket], 1)
}

func (self *WriteStats) UpdateLatencyStats(latencyUs int64) {
	bucket := 0
	if latencyUs < 1024 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
	}
	if bucket >= len(self.WriteLatencyStats) {
		bucket = len(self.WriteLatencyStats) - 1
	}
	atomic.AddInt64(&self.WriteLatencyStats[bucket], 1)
}

func (self *WriteStats) UpdateWriteStats(vSize int64, latencyUs int64) {
	self.UpdateSizeStats(vSize)
	self.UpdateLatencyStats(latencyUs)
}

func (self *WriteStats) Copy() *WriteStats {
	var s WriteStats
	for i := 0; i < len(self.ValueSizeStats); i++ {
		s.ValueSizeStats[i] = atomic.LoadInt64(&self.ValueSizeStats[i])
	}
	for i := 0; i < len(self.WriteLatencyStats); i++ {
		s.WriteLatencyStats[i] = atomic.LoadInt64(&self.WriteLatencyStats[i])
	}
	return &s
}

type TableStats struct {
	Name   string `json:"name"`
	KeyNum int64  `json:"key_num"`
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

type ScanStats struct {
	ScanCount uint64 `json:"scan_count"`
	// <1024us, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s
	ScanLatencyStats [16]int64 `json:"scan_latency_stats"`
}

func (self *ScanStats) IncScanCount() {
	atomic.AddUint64(&self.ScanCount, 1)
}

func (self *ScanStats) UpdateLatencyStats(latencyUs int64) {
	bucket := 0
	if latencyUs < 1024 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
	}
	if bucket >= len(self.ScanLatencyStats) {
		bucket = len(self.ScanLatencyStats) - 1
	}
	atomic.AddInt64(&self.ScanLatencyStats[bucket], 1)
}

func (self *ScanStats) UpdateScanStats(latencyUs int64) {
	self.IncScanCount()
	self.UpdateLatencyStats(latencyUs)
}

func (self *ScanStats) Copy() *ScanStats {
	var s ScanStats
	s.ScanCount = atomic.LoadUint64(&self.ScanCount)
	for i := 0; i < len(self.ScanLatencyStats); i++ {
		s.ScanLatencyStats[i] = atomic.LoadInt64(&self.ScanLatencyStats[i])
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

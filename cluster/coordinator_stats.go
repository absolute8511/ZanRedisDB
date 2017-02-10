package cluster

import (
	"sync"
	"sync/atomic"
)

const maxNumCounters = 1024

var (
	cnames       = make([]string, maxNumCounters)
	counters     = make([]uint64, maxNumCounters)
	curCounterID = new(uint32)
)

func AddCounter(name string) uint32 {
	id := atomic.AddUint32(curCounterID, 1) - 1
	if id >= maxNumCounters {
		panic("Too many counters")
	}
	cnames[id] = name
	return id
}

func IncCounter(id uint32) {
	atomic.AddUint64(&counters[id], 1)
}

func IncCounterBy(id uint32, amount uint64) {
	atomic.AddUint64(&counters[id], amount)
}

func getAllCounters() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curCounterID))

	for i := 0; i < numIDs; i++ {
		ret[cnames[i]] = atomic.LoadUint64(&counters[i])
	}

	return ret
}

type CoordErrStatsData struct {
	NamespaceCoordMissingError int64
	LocalErr                   int64
	OtherCoordErrs             map[string]int64
}

type CoordErrStats struct {
	sync.Mutex
	CoordErrStatsData
}

func newCoordErrStats() *CoordErrStats {
	s := &CoordErrStats{}
	s.OtherCoordErrs = make(map[string]int64, 100)
	return s
}

func (self *CoordErrStats) GetCopy() *CoordErrStatsData {
	var ret CoordErrStatsData
	self.Lock()
	ret = coordErrStats.CoordErrStatsData
	ret.OtherCoordErrs = make(map[string]int64, len(self.OtherCoordErrs))
	for k, v := range self.OtherCoordErrs {
		ret.OtherCoordErrs[k] = v
	}
	self.Unlock()
	return &ret
}

func (self *CoordErrStats) incLocalErr() {
	atomic.AddInt64(&self.LocalErr, 1)
}

func (self *CoordErrStats) incNamespaceCoordMissingErr() {
	atomic.AddInt64(&self.NamespaceCoordMissingError, 1)
}

func (self *CoordErrStats) incCoordErr(e *CoordErr) {
	if e == nil || !e.HasError() {
		return
	}
	self.Lock()
	cnt := self.OtherCoordErrs[e.ErrMsg]
	cnt++
	self.OtherCoordErrs[e.ErrMsg] = cnt
	self.Unlock()
}

var coordErrStats = newCoordErrStats()

type ISRStat struct {
	HostName string `json:"hostname"`
	NodeID   string `json:"node_id"`
}

type NamespaceCoordStat struct {
	Node      string    `json:"node"`
	Name      string    `json:"name"`
	Partition int       `json:"partition"`
	ISRStats  []ISRStat `json:"isr_stats"`
}

type CoordStats struct {
	ErrStats     CoordErrStatsData
	NsCoordStats []NamespaceCoordStat `json:"ns_coord_stats"`
}

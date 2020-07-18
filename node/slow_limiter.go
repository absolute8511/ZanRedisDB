package node

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/metric"

	ps "github.com/prometheus/client_golang/prometheus"
)

var enableSlowLimiterTest = false

func EnableSlowLimiterTest(t bool) {
	enableSlowLimiterTest = t
}

// ErrSlowLimiterRefused indicated the write request is slow while applying so it is refused to avoid
// slow down other write.
var ErrSlowLimiterRefused = errors.New("refused by slow limiter")

type slowLevelT int

const (
	minSlowLevel slowLevelT = iota
	midSlowLevel
	verySlowLevel
	maxSlowLevel
)

const (
	maxSlowThreshold   = 300
	heavySlowThreshold = 250
	midSlowThreshold   = 60
	smallSlowThreshold = 20
	slowQueueThreshold = 5
)

var SlowRefuseCostMs = int64(600)
var SlowHalfOpenSec = int64(15)
var maybeSlowCmd map[string]bool
var slowQueueCostMs = int64(250)

func init() {
	maybeSlowCmd = make(map[string]bool, 100)
	maybeSlowCmd["spop"] = true
	maybeSlowCmd["zremrangebyrank"] = true
	maybeSlowCmd["zremrangebyscore"] = true
	maybeSlowCmd["zremrangebylex"] = true
	maybeSlowCmd["ltrim"] = true
	// remove below if compact ttl is enabled by default
	maybeSlowCmd["sclear"] = true
	maybeSlowCmd["zclear"] = true
	maybeSlowCmd["lclear"] = true
	maybeSlowCmd["hclear"] = true
}

func IsMaybeSlowWriteCmd(cmd string) bool {
	_, ok := maybeSlowCmd[cmd]
	return ok
}

func RegisterSlowConfChanged() {
	common.RegisterConfChangedHandler(common.ConfSlowLimiterRefuseCostMs, func(v interface{}) {
		iv, ok := v.(int)
		if ok {
			atomic.StoreInt64(&SlowRefuseCostMs, int64(iv))
		}
	})
	common.RegisterConfChangedHandler(common.ConfSlowLimiterHalfOpenSec, func(v interface{}) {
		iv, ok := v.(int)
		if ok {
			atomic.StoreInt64(&SlowHalfOpenSec, int64(iv))
		}
	})
}

type SlowWaitDone struct {
	c chan struct{}
}

func (swd *SlowWaitDone) Done() {
	if swd.c == nil {
		return
	}
	select {
	case <-swd.c:
	default:
	}
}

// SlowLimiter is used to limit some slow write command to avoid raft blocking
type SlowLimiter struct {
	ns               string
	slowCounter      int64
	slowQueueCounter int64

	limiterOn    int32
	mutex        sync.RWMutex
	slowHistorys [maxSlowLevel]map[string]int64
	lastSlowTs   int64
	stopC        chan struct{}
	wg           sync.WaitGroup
	// some slow write should wait in queue until others returned from raft apply
	slowWaitQueue [maxSlowLevel]chan struct{}
}

func NewSlowLimiter(ns string) *SlowLimiter {
	var his [maxSlowLevel]map[string]int64
	var q [maxSlowLevel]chan struct{}
	l := 100
	for i := 0; i < len(his); i++ {
		his[i] = make(map[string]int64)
		q[i] = make(chan struct{}, l+3)
		l = l / 5
	}

	sl := &SlowLimiter{
		ns:            ns,
		limiterOn:     int32(common.GetIntDynamicConf(common.ConfSlowLimiterSwitch)),
		slowWaitQueue: q,
		slowHistorys:  his,
	}
	return sl
}

func (sl *SlowLimiter) Start() {
	sl.stopC = make(chan struct{})
	sl.wg.Add(1)
	go sl.run(sl.stopC)
}

func (sl *SlowLimiter) Stop() {
	if sl.stopC != nil {
		close(sl.stopC)
		sl.stopC = nil
	}
	sl.wg.Wait()
}

func (sl *SlowLimiter) run(stopC chan struct{}) {
	defer sl.wg.Done()
	checkInterval := time.Second * 2
	if enableSlowLimiterTest {
		checkInterval = checkInterval / 4
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// decr slow counter more quickly to reduce the time
			// in mid or heavy slow state to avoid refuse too much write with lower cost
			old := atomic.LoadInt64(&sl.slowCounter)
			nodeLog.Debugf("current slow %v , last slow ts: %v",
				old, atomic.LoadInt64(&sl.lastSlowTs))
			decr := -1
			if old >= heavySlowThreshold {
				decr = -10
			} else if old >= midSlowThreshold {
				decr = -2
			}
			// speed up for test
			if enableSlowLimiterTest && old > 10 {
				decr *= 3
			}
			n := atomic.AddInt64(&sl.slowCounter, int64(decr))
			if old >= smallSlowThreshold && n < smallSlowThreshold {
				// we only clear slow stats while we changed from real slow to no slow
				nodeLog.Infof("the apply limiter is changed from slow %v to no slow: %v , last slow ts: %v",
					old, n, atomic.LoadInt64(&sl.lastSlowTs))
				sl.clearSlows()
			}
			if n < 0 {
				atomic.AddInt64(&sl.slowCounter, int64(-1*decr))
			}
		case <-stopC:
			return
		}
	}
}

func (sl *SlowLimiter) testSlowWrite1s(cmd redcon.Command, ts int64) (interface{}, error) {
	time.Sleep(time.Second)
	return nil, nil
}
func (sl *SlowLimiter) testSlowWrite100ms(cmd redcon.Command, ts int64) (interface{}, error) {
	time.Sleep(time.Millisecond * 100)
	return nil, nil
}
func (sl *SlowLimiter) testSlowWrite50ms(cmd redcon.Command, ts int64) (interface{}, error) {
	time.Sleep(time.Millisecond * 50)
	return nil, nil
}
func (sl *SlowLimiter) testSlowWrite5ms(cmd redcon.Command, ts int64) (interface{}, error) {
	time.Sleep(time.Millisecond * 5)
	return nil, nil
}

func (sl *SlowLimiter) TurnOn() {
	atomic.StoreInt32(&sl.limiterOn, 1)
}

func (sl *SlowLimiter) TurnOff() {
	atomic.StoreInt32(&sl.limiterOn, 0)
}

func (sl *SlowLimiter) isOn() bool {
	return atomic.LoadInt32(&sl.limiterOn) > 0
}

func (sl *SlowLimiter) MarkHeavySlow() {
	atomic.StoreInt64(&sl.slowCounter, maxSlowThreshold)
	atomic.StoreInt64(&sl.lastSlowTs, time.Now().UnixNano())
}

func (sl *SlowLimiter) clearSlows() {
	if !sl.isOn() {
		return
	}
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	atomic.StoreInt64(&sl.slowQueueCounter, 0)
	for i := 0; i < len(sl.slowHistorys); i++ {
		if len(sl.slowHistorys[i]) > 0 {
			sl.slowHistorys[i] = make(map[string]int64)
		}
	}
}

func (sl *SlowLimiter) MaybeAddSlow(ts int64, cost time.Duration, cmd string, prefix string) {
	if cost < time.Millisecond*time.Duration(atomic.LoadInt64(&SlowRefuseCostMs)) {
		// while we are in some slow down state, slow write will be refused,
		// while in half open, some history slow write will be passed to do
		// slow check again, in this way we need check the history to
		// identify the possible slow write more fast.
		if cost >= time.Millisecond*time.Duration(slowQueueCostMs) {
			atomic.AddInt64(&sl.slowQueueCounter, 1)
		}
		if cost < getCostThresholdForSlowLevel(midSlowLevel) {
			return
		}
		cnt := atomic.LoadInt64(&sl.slowCounter)
		if cnt < smallSlowThreshold {
			return
		}
		isSlow, _ := sl.isHistorySlow(cmd, prefix, cnt, minSlowLevel)
		if !isSlow {
			return
		}
	}
	sl.AddSlow(ts)
}

// return isslow and issmallslow
func (sl *SlowLimiter) isHistorySlow(cmd, prefix string, sc int64, ignoreSlowLevel slowLevelT) (bool, slowLevelT) {
	feat := cmd + " " + prefix
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	for lv := minSlowLevel; lv < maxSlowLevel; lv++ {
		if lv <= ignoreSlowLevel {
			continue
		}
		slow := sl.slowHistorys[lv]
		cnt, ok := slow[feat]
		if lv >= verySlowLevel {
			if ok && cnt > 2 {
				return true, lv
			}
		} else if sc >= midSlowThreshold && lv >= midSlowLevel {
			if ok && cnt > 4 {
				return true, lv
			}
		} else if sc >= heavySlowThreshold && lv >= minSlowLevel {
			if ok && cnt > 16 {
				return true, lv
			}
		}
	}
	return false, 0
}

func (sl *SlowLimiter) AddSlow(ts int64) {
	atomic.StoreInt64(&sl.lastSlowTs, ts)
	sl.addCounterOnly()
}

func (sl *SlowLimiter) addCounterOnly() {
	cnt := atomic.AddInt64(&sl.slowCounter, 1)
	atomic.AddInt64(&sl.slowQueueCounter, 1)
	if cnt > maxSlowThreshold {
		atomic.AddInt64(&sl.slowCounter, -1)
	}
}

func (sl *SlowLimiter) PreWaitQueue(ctx context.Context, cmd string, prefix string) (*SlowWaitDone, error) {
	feat := cmd + " " + prefix
	slv := slowLevelT(-1)
	if IsMaybeSlowWriteCmd(cmd) {
		slv = verySlowLevel
	} else {
		sl.mutex.RLock()
		for lv := verySlowLevel; lv >= 0; lv-- {
			slow := sl.slowHistorys[lv]
			cnt, ok := slow[feat]
			if ok && cnt > 2 {
				slv = lv
				break
			}
		}
		sl.mutex.RUnlock()
	}
	if slv >= maxSlowLevel || slv < 0 {
		return nil, nil
	}
	wq := sl.slowWaitQueue[slv]
	begin := time.Now()
	select {
	case <-ctx.Done():
		metric.SlowLimiterRefusedCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
		return nil, ctx.Err()
	case wq <- struct{}{}:
	}
	cost := time.Since(begin)
	if cost >= time.Millisecond {
		metric.SlowLimiterQueuedCost.With(ps.Labels{
			"namespace": sl.ns,
			"table":     prefix,
			"cmd":       cmd,
		}).Observe(float64(cost.Milliseconds()))
	}
	metric.SlowLimiterQueuedCnt.With(ps.Labels{
		"table":      prefix,
		"cmd":        cmd,
		"slow_level": getSlowLevelDesp(slv),
	}).Inc()
	metric.QueueLen.With(ps.Labels{
		"namespace":  sl.ns,
		"queue_name": "slow_wait_queue_" + getSlowLevelDesp(slv),
	}).Set(float64(len(wq)))
	return &SlowWaitDone{wq}, nil
}

func (sl *SlowLimiter) CanPass(ts int64, cmd string, prefix string) bool {
	if prefix == "" {
		return true
	}
	if !sl.isOn() {
		return true
	}
	sc := atomic.LoadInt64(&sl.slowCounter)
	if sc < smallSlowThreshold {
		return true
	}
	if ts > atomic.LoadInt64(&sl.lastSlowTs)+time.Second.Nanoseconds()*SlowHalfOpenSec {
		return true
	}
	if isSlow, _ := sl.isHistorySlow(cmd, prefix, sc, -1); isSlow {
		// the write is refused, means it may slow down the raft loop if we passed,
		// so we need add counter here even we refused it.
		// However, we do not update timestamp for slow, so we can clear it if it become
		// no slow while in half open state.
		sl.addCounterOnly()
		metric.SlowLimiterRefusedCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
		return false
	}
	return true
}

func getCostThresholdForSlowLevel(slv slowLevelT) time.Duration {
	if slv >= verySlowLevel {
		return time.Millisecond * 100
	}
	if slv >= midSlowLevel {
		return time.Millisecond * 50
	}
	if slv >= minSlowLevel {
		return time.Millisecond * 10
	}
	return 0
}

func getSlowLevelDesp(slv slowLevelT) string {
	return strconv.Itoa(int(slv))
}

func getSlowLevelFromCost(cost time.Duration) slowLevelT {
	if cost >= time.Millisecond*100 {
		return verySlowLevel
	}
	if cost >= time.Millisecond*50 {
		return midSlowLevel
	}
	if cost >= time.Millisecond*10 {
		return minSlowLevel
	}
	return -1
}

func (sl *SlowLimiter) RecordSlowCmd(cmd string, prefix string, cost time.Duration) {
	if prefix == "" || cmd == "" {
		return
	}
	slv := getSlowLevelFromCost(cost)
	if slv < minSlowLevel || slv >= maxSlowLevel {
		return
	}
	if slv == verySlowLevel {
		metric.SlowWrite100msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	} else if slv == midSlowLevel {
		metric.SlowWrite50msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	} else if slv == minSlowLevel {
		metric.SlowWrite10msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	}
	if !sl.isOn() {
		return
	}
	sc := atomic.LoadInt64(&sl.slowCounter)
	qc := atomic.LoadInt64(&sl.slowQueueCounter)
	if sc < smallSlowThreshold && qc < slowQueueThreshold {
		return
	}
	feat := cmd + " " + prefix
	sl.mutex.Lock()
	slow := sl.slowHistorys[slv]
	old, ok := slow[feat]
	if !ok {
		old = 0
	}
	old++
	slow[feat] = old
	sl.mutex.Unlock()
}

package node

import (
	"errors"
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

const (
	maxSlowThreshold   = 300
	heavySlowThreshold = 250
	midSlowThreshold   = 60
	smallSlowThreshold = 20
	SlowRefuseCost     = time.Millisecond * 600
	SlowHalfOpen       = time.Minute / 2
)

// SlowLimiter is used to limit some slow write command to avoid raft blocking
type SlowLimiter struct {
	slowCounter int64

	limiterOn  int32
	mutex      sync.RWMutex
	slow100s   map[string]int64
	slow50s    map[string]int64
	slow10s    map[string]int64
	lastSlowTs int64
	stopC      chan struct{}
	wg         sync.WaitGroup
}

func NewSlowLimiter() *SlowLimiter {
	return &SlowLimiter{
		limiterOn: int32(common.GetIntDynamicConf(common.ConfSlowLimiterSwitch)),
		slow100s:  make(map[string]int64),
		slow50s:   make(map[string]int64),
		slow10s:   make(map[string]int64),
	}
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
	ticker := time.NewTicker(time.Second * 2)
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
	if len(sl.slow100s) > 0 {
		sl.slow100s = make(map[string]int64)
	}
	if len(sl.slow50s) > 0 {
		sl.slow50s = make(map[string]int64)
	}
	if len(sl.slow10s) > 0 {
		sl.slow10s = make(map[string]int64)
	}
}

func (sl *SlowLimiter) MaybeAddSlow(ts int64, cost time.Duration, cmd string, prefix string) {
	if cost < SlowRefuseCost {
		// while we are in some slow down state, slow write will be refused,
		// while in half open, some history slow write will be passed to allow
		// slow check again, in this way we need check the history to
		// identify the possible slow write more fast.
		if cost < time.Millisecond*100 {
			return
		}
		cnt := atomic.LoadInt64(&sl.slowCounter)
		if cnt <= smallSlowThreshold {
			return
		}
		feat := cmd + " " + prefix
		sl.mutex.RLock()
		cnt, ok := sl.slow100s[feat]
		sl.mutex.RUnlock()
		if !ok || cnt <= 1 {
			return
		}
	}
	sl.AddSlow(ts)
}

func (sl *SlowLimiter) AddSlow(ts int64) {
	atomic.StoreInt64(&sl.lastSlowTs, ts)
	cnt := atomic.AddInt64(&sl.slowCounter, 1)
	if cnt > maxSlowThreshold {
		atomic.AddInt64(&sl.slowCounter, -1)
	}
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
	if ts > atomic.LoadInt64(&sl.lastSlowTs)+SlowHalfOpen.Nanoseconds() {
		return true
	}
	feat := cmd + " " + prefix
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	passed := true
	defer func() {
		if !passed {
			metric.SlowLimiterRefusedCnt.With(ps.Labels{
				"table": prefix,
				"cmd":   cmd,
			}).Inc()
		}
	}()
	cnt, ok := sl.slow100s[feat]
	if ok && cnt > 1 {
		passed = false
		return passed
	}
	if sc >= midSlowThreshold {
		cnt, ok := sl.slow50s[feat]
		if ok && cnt > 2 {
			passed = false
			return passed
		}
	}
	if sc >= heavySlowThreshold {
		cnt, ok := sl.slow10s[feat]
		if ok && cnt > 10 {
			passed = false
			return passed
		}
	}
	return passed
}

func (sl *SlowLimiter) RecordSlowCmd(cmd string, prefix string, cost time.Duration) {
	if prefix == "" || cmd == "" {
		return
	}
	slowKind := 0
	if cost >= time.Millisecond*100 {
		slowKind = 100
		metric.SlowWrite100msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	} else if cost >= time.Millisecond*50 {
		slowKind = 50
		metric.SlowWrite50msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	} else if cost >= time.Millisecond*10 {
		slowKind = 10
		metric.SlowWrite10msCnt.With(ps.Labels{
			"table": prefix,
			"cmd":   cmd,
		}).Inc()
	} else {
		return
	}
	if !sl.isOn() {
		return
	}
	sc := atomic.LoadInt64(&sl.slowCounter)
	if sc < smallSlowThreshold {
		return
	}
	feat := cmd + " " + prefix
	sl.mutex.Lock()
	slow := sl.slow100s
	if slowKind == 50 {
		slow = sl.slow50s
	} else if slowKind == 10 {
		slow = sl.slow10s
	}
	old, ok := slow[feat]
	if !ok {
		old = 0
	}
	old++
	slow[feat] = old
	sl.mutex.Unlock()
}

package slow

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/common"
)

// merged and formatted slow logs for write or large collections
// use slow-level to control output different slow logs
const (
	collectionMinLenForLog = 128
	collectionLargeLen     = 5000
	dbWriteSlow            = time.Millisecond * 100
)

var sl = common.NewLevelLogger(common.LOG_INFO, common.NewGLogger())

func SetLogger(level int32, logger common.Logger) {
	sl.SetLevel(level)
	sl.Logger = logger
}

var slowLogLevel int32

func ChangeSlowLogLevel(lv int) {
	atomic.StoreInt32(&slowLogLevel, int32(lv))
}

func slowLogLv() int32 {
	return atomic.LoadInt32(&slowLogLevel)
}

var logCollTimes [32]*int64

func init() {
	for i := 0; i < len(logCollTimes); i++ {
		var d int64
		logCollTimes[i] = &d
	}
}

func getIndex(si SlowLogInfo) int {
	index := 0
	total := len(si.Scope)
	if total > 0 {
		index += int(si.Scope[total-1])
	}
	if total > 1 {
		index += int(si.Scope[total-2])
	}
	return index
}

func checkLastLogCollTime(si SlowLogInfo) bool {
	index := getIndex(si)
	t := logCollTimes[index%len(logCollTimes)]
	return atomic.LoadInt64(t)+time.Second.Nanoseconds() <= time.Now().UnixNano()
}

func updateLastLogCollTime(si SlowLogInfo) {
	t := logCollTimes[getIndex(si)%len(logCollTimes)]
	atomic.StoreInt64(t, time.Now().UnixNano())
}

type SlowLogInfo struct {
	Scope string
	Key   string
	Note  string
}

func NewSlowLogInfo(scope string, key string, note string) SlowLogInfo {
	return SlowLogInfo{
		Scope: scope,
		Key:   key,
		Note:  note,
	}
}

// LogLargeColl
// LogSlowWrite
// LogLargeValue
// LogLargeBatch

func LogSlowDBWrite(cost time.Duration, si SlowLogInfo) (string, bool) {
	if slowLogLv() < 0 {
		return "", false
	}

	if cost >= dbWriteSlow || slowLogLv() > common.LOG_DETAIL ||
		(slowLogLv() >= common.LOG_INFO && cost >= dbWriteSlow/2) {
		str := fmt.Sprintf("[SLOW_LOGS] db slow write command in scope %v, cost: %v, key: %v, note: %v",
			si.Scope, cost, si.Key, si.Note)

		sl.InfoDepth(1, str)
		return str, true
	}
	return "", false
}

func LogDebugSlowWrite(cost time.Duration, thres time.Duration, lvFor int32, si SlowLogInfo) (string, bool) {
	if slowLogLv() < common.LOG_DEBUG {
		return "", false
	}
	if cost >= thres && slowLogLv() >= int32(lvFor) {
		str := fmt.Sprintf("[SLOW_LOGS] debug slow write in scope %v, cost: %v, note: %v",
			si.Scope, cost, si.Note)
		sl.InfoDepth(1, str)
		return str, true
	}
	return "", false
}

func LogSlowForSteps(thres time.Duration, lvFor int32, si SlowLogInfo, costList ...time.Duration) (string, bool) {
	if len(costList) == 0 {
		return "", false
	}
	if slowLogLv() < 0 {
		return "", false
	}
	if costList[len(costList)-1] >= thres && slowLogLv() >= int32(lvFor) {
		str := fmt.Sprintf("[SLOW_LOGS] steps slow in scope %v, cost list: %v, note: %v",
			si.Scope, costList, si.Note)
		sl.InfoDepth(1, str)
		return str, true
	}
	return "", false
}

func LogLargeCollection(sz int, si SlowLogInfo) (string, bool) {
	if sz < collectionMinLenForLog {
		return "", false
	}
	lv := slowLogLv()
	if lv < 0 {
		return "", false
	}
	if sz >= collectionLargeLen {
		if lv >= common.LOG_INFO || checkLastLogCollTime(si) {
			str := fmt.Sprintf("[SLOW_LOGS] large collection in scope %v, size: %v, key: %v, note: %v",
				si.Scope, sz, si.Key, si.Note)
			sl.InfoDepth(1, str)
			updateLastLogCollTime(si)
			return str, true
		}
	}
	if lv >= common.LOG_DETAIL ||
		(lv >= common.LOG_INFO && sz >= collectionMinLenForLog*4 && checkLastLogCollTime(si)) ||
		(lv >= common.LOG_DEBUG && sz >= collectionMinLenForLog*2) {
		str := fmt.Sprintf("[SLOW_LOGS] maybe large collection in scope %v, size: %v, key: %v, note: %v",
			si.Scope, sz, si.Key, si.Note)
		sl.InfoDepth(1, str)
		if lv <= common.LOG_INFO {
			updateLastLogCollTime(si)
		}
		return str, true
	}
	return "", false
}

func LogLargeValue() (string, bool) {
	return "", false
}

func LogLargeBatchWrite() (string, bool) {
	return "", false
}

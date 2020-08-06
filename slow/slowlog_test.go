package slow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func TestSlowLogLevel(t *testing.T) {
	SetLogger(common.LOG_INFO, common.NewDefaultLogger("test"))
	str, logged := LogLargeCollection(collectionLargeLen, NewSlowLogInfo("", "test", ""))
	t.Log(str)
	assert.Equal(t, true, logged)
	str, logged = LogLargeCollection(collectionMinLenForLog*2, NewSlowLogInfo("", "test", ""))
	t.Log(str)
	assert.Equal(t, false, logged)
	str, logged = LogSlowDBWrite(dbWriteSlow, NewSlowLogInfo("test", "testkey", ""))
	t.Log(str)
	assert.Equal(t, true, logged)
	str, logged = LogSlowForSteps(dbWriteSlow, 0, NewSlowLogInfo("test", "testkey", ""), dbWriteSlow/2, dbWriteSlow)
	t.Log(str)
	assert.Equal(t, true, logged)

	str, logged = LogSlowForSteps(dbWriteSlow, 1, NewSlowLogInfo("test", "testkey", ""), dbWriteSlow/2, dbWriteSlow)
	t.Log(str)
	assert.Equal(t, false, logged)

	ChangeSlowLogLevel(-1)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("", "test", ""))
	t.Log(str)
	assert.Equal(t, false, logged)
	assert.Equal(t, "", str)
	str, logged = LogSlowDBWrite(dbWriteSlow, NewSlowLogInfo("test", "testkey", ""))
	t.Log(str)
	assert.Equal(t, "", str)

	str, logged = LogSlowForSteps(dbWriteSlow, 1, NewSlowLogInfo("test", "testkey", ""), dbWriteSlow/2, dbWriteSlow)
	t.Log(str)
	assert.Equal(t, "", str)
	assert.Equal(t, false, logged)

	ChangeSlowLogLevel(int(common.LOG_DEBUG))
	str, logged = LogLargeCollection(collectionMinLenForLog*2, NewSlowLogInfo("", "test", ""))
	t.Log(str)
	assert.Equal(t, true, logged)

	str, logged = LogSlowDBWrite(dbWriteSlow, NewSlowLogInfo("test", "testkey", ""))
	t.Log(str)
	assert.Equal(t, true, logged)

	str, logged = LogSlowForSteps(dbWriteSlow, common.LOG_DEBUG, NewSlowLogInfo("test", "testkey", ""), dbWriteSlow/2, dbWriteSlow)
	t.Log(str)
	assert.Equal(t, true, logged)
	ChangeSlowLogLevel(0)
	// test dump log reduce in second
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	t.Log(str)
	assert.Equal(t, true, logged)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	t.Log(str)
	assert.Equal(t, false, logged)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	t.Log(str)
	assert.Equal(t, false, logged)
	time.Sleep(time.Second)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	t.Log(str)
	assert.Equal(t, true, logged)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	t.Log(str)
	assert.Equal(t, false, logged)
}

func TestSlowLogRemote(t *testing.T) {
	SetLogger(common.LOG_INFO, common.NewDefaultLogger("test"))
	SetRemoteLogger("127.0.0.1:5140")
	str, logged := LogSlowDBWrite(dbWriteSlow, NewSlowLogInfo("test", "testkey", "slow db write note"))
	t.Log(str)
	assert.Equal(t, true, logged)
	time.Sleep(time.Second * 2)
	str, logged = LogSlowForSteps(dbWriteSlow/4, common.LOG_ERR, NewSlowLogInfo("test", "testkey", "slow for steps note"), dbWriteSlow/2, dbWriteSlow)
	t.Log(str)
	assert.Equal(t, true, logged)
	time.Sleep(time.Second * 2)
	str, logged = LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", "slow for large note"))
	t.Log(str)
	assert.Equal(t, true, logged)
	// wait remote log flush
	time.Sleep(time.Second * 10)
}

func BenchmarkLogLarge(b *testing.B) {
	SetLogger(common.LOG_INFO, common.NewDefaultLogger("test"))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		LogLargeCollection(collectionLargeLen, NewSlowLogInfo("scope_test", "test", ""))
	}
}

package slow

import (
	"testing"

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
}

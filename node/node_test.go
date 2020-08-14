package node

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/rockredis"
	"github.com/youzan/ZanRedisDB/slow"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
)

func TestMain(m *testing.M) {
	SetLogger(int32(common.LOG_INFO), nil)
	rockredis.SetLogger(int32(common.LOG_INFO), nil)
	slow.SetLogger(int32(common.LOG_INFO), nil)
	engine.SetLogger(int32(common.LOG_INFO), nil)
	rafthttp.SetLogger(int32(common.LOG_INFO), nil)
	flag.Parse()
	if testing.Verbose() {
		common.InitDefaultForGLogger("")
		SetLogLevel(int(common.LOG_DETAIL))
		rockredis.SetLogLevel(int32(common.LOG_DETAIL))
		engine.SetLogLevel(int32(common.LOG_DETAIL))
	}
	ret := m.Run()
	os.Exit(ret)
}

func TestWaitReqPools(t *testing.T) {
	wrPools := newWaitReqPoolArray()

	wr := wrPools.getWaitReq(1)
	assert.Equal(t, 1, cap(wr.reqs.Reqs))
	//assert.Equal(t, minPoolIDLen, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen)
	//assert.Equal(t, minPoolIDLen, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen + 1)
	//assert.Equal(t, minPoolIDLen*2, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen * 2)
	//assert.Equal(t, minPoolIDLen*2, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen*2 + 1)
	//assert.Equal(t, minPoolIDLen*2*2, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen * 2 * 2)
	//assert.Equal(t, minPoolIDLen*2*2, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen*2*2 + 1)
	//assert.Equal(t, minPoolIDLen*2*2*2, cap(wr.ids))
	//wr = wrPools.getWaitReq(minPoolIDLen * 2 * 2 * 2)
	//assert.Equal(t, minPoolIDLen*2*2*2, cap(wr.ids))
	wr.release(true)
	//wr = wrPools.getWaitReq(maxPoolIDLen)
	//assert.Equal(t, minPoolIDLen*int(math.Pow(float64(2), float64(waitPoolSize-1))), cap(wr.ids))
	//wr.release()
	wr = wrPools.getWaitReq(maxPoolIDLen + 1)
	assert.Equal(t, maxPoolIDLen+1, cap(wr.reqs.Reqs))
	wr.release(true)
}

func BenchmarkBatchRequestMarshal(b *testing.B) {
	br := &BatchInternalRaftRequest{}
	br.ReqId = 1
	irr := InternalRaftRequest{
		Data: make([]byte, 100),
	}
	irr.Header.Timestamp = time.Now().UnixNano()
	br.Reqs = append(br.Reqs, irr)

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Marshal()
		}
	})
}

func BenchmarkRequestMarshal(b *testing.B) {
	irr := InternalRaftRequest{
		Data: make([]byte, 100),
	}
	irr.Header.Timestamp = time.Now().UnixNano()

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			irr.Marshal()
		}
	})
}

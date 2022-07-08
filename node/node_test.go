package node

import (
	"context"
	"errors"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/pkg/idutil"
	"github.com/youzan/ZanRedisDB/pkg/wait"
	"github.com/youzan/ZanRedisDB/rockredis"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
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

func TestProposeWaitMoreThanOnceTrigger(t *testing.T) {
	var reqList BatchInternalRaftRequest
	w := wait.New()
	reqIDGen := idutil.NewGenerator(uint16(1), time.Now())
	reqList.ReqId = reqIDGen.Next()
	// must register before propose
	wr := w.Register(reqList.ReqId)
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)

	var futureRsp FutureRsp
	futureRsp.waitFunc = func() (interface{}, error) {
		var rsp interface{}
		var ok bool
		var err error
		// will always return a response, timed out or get a error
		select {
		case <-ctx.Done():
			err = ctx.Err()
			if err == context.Canceled {
				// proposal canceled can be caused by leader transfer or no leader
				err = ErrProposalCanceled
			}
			w.Trigger(reqList.ReqId, err)
			rsp = err
		case <-wr.WaitC():
			rsp = wr.GetResult()
		}
		cancel()
		if err, ok = rsp.(error); ok {
			rsp = nil
			//nd.rn.Infof("request return error: %v, %v", req.String(), err.Error())
		} else {
			err = nil
		}
		return rsp, err
	}
	_, err := futureRsp.WaitRsp()
	assert.Equal(t, context.DeadlineExceeded, err)
	w.Trigger(reqList.ReqId, errors.New("unexpected"))
	_, err = futureRsp.WaitRsp()
	assert.Equal(t, context.DeadlineExceeded, err)
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

package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWaitReqPools(t *testing.T) {
	wrPools := newWaitReqPoolArray()

	wr := wrPools.getWaitReq(1)
	assert.Equal(t, minPoolIDLen, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen)
	assert.Equal(t, minPoolIDLen, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen + 1)
	assert.Equal(t, minPoolIDLen*2, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen * 2)
	assert.Equal(t, minPoolIDLen*2, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen*2 + 1)
	assert.Equal(t, minPoolIDLen*2*2, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen * 2 * 2)
	assert.Equal(t, minPoolIDLen*2*2, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen*2*2 + 1)
	assert.Equal(t, minPoolIDLen*2*2*2, cap(wr.ids))
	wr = wrPools.getWaitReq(minPoolIDLen * 2 * 2 * 2)
	assert.Equal(t, minPoolIDLen*2*2*2, cap(wr.ids))
	wr.release()
	wr = wrPools.getWaitReq(maxPoolIDLen)
	assert.Equal(t, minPoolIDLen*int(math.Pow(float64(2), float64(waitPoolSize-1))), cap(wr.ids))
	wr.release()
	wr = wrPools.getWaitReq(maxPoolIDLen + 1)
	assert.Equal(t, maxPoolIDLen+1, cap(wr.ids))
	wr.release()
}

package metric

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollSizeHeap(t *testing.T) {
	h := NewCollSizeHeap(DefaultHeapCapacity)
	for i := 0; i < minCollSizeInHeap*2; i++ {
		k := []byte(strconv.Itoa(i))
		h.Update(k, i)
	}
	assert.Equal(t, minCollSizeInHeap, len(h.TopKeys()))
	keys := h.TopKeys()
	assert.True(t, keys[0].Cnt <= keys[1].Cnt)
	assert.True(t, keys[len(keys)-2].Cnt <= keys[len(keys)-1].Cnt)
	assert.Equal(t, int32(minCollSizeInHeap*2-1), keys[len(keys)-1].Cnt)
	assert.Equal(t, strconv.Itoa(int(keys[len(keys)-1].Cnt)), keys[len(keys)-1].Key)

	oldCnt := len(keys)
	h.Update([]byte(keys[len(keys)-1].Key), minCollSizeInHeap-1)
	// should be removed since under minCollSizeInHeap
	keys = h.TopKeys()
	assert.Equal(t, int32(minCollSizeInHeap*2-1-1), keys[len(keys)-1].Cnt)
	assert.Equal(t, oldCnt-1, len(keys))
}

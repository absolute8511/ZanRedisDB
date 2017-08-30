package common

import (
	"container/heap"
	"strconv"
	"testing"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

func TestSortHindexRespForStr(t *testing.T) {
	respList := make(SearchResultHeap, 50)

	for i := 0; i < 50; i++ {
		b := byte(rand.Int31n(255))
		var bytesV []byte
		bytesV = append(bytesV, b)
		respList[i] = &HIndexRespWithValues{
			PKey:   []byte(strconv.Itoa(i)),
			IndexV: bytesV,
		}
		t.Log(respList[i])
	}
	heap.Init(&respList)
	for i := 0; i < 50; i++ {
		b := byte(rand.Int31n(255))
		var bytesV []byte
		bytesV = append(bytesV, b)
		v := &HIndexRespWithValues{
			PKey:   []byte(strconv.Itoa(i)),
			IndexV: bytesV,
		}
		heap.Push(&respList, v)
		t.Log(respList[i])
	}
	var lastV *HIndexRespWithValues
	for respList.Len() > 0 {
		v := heap.Pop(&respList).(*HIndexRespWithValues)
		t.Log(v)
		if lastV != nil {
			assert.True(t, string(v.IndexV.([]byte)) >= string(lastV.IndexV.([]byte)), "should sorted")
		}
		lastV = v
	}
}

func TestSortHindexRespForInt(t *testing.T) {
	respList := make(SearchResultHeap, 50)
	for i := 0; i < 50; i++ {
		respList[i] = &HIndexRespWithValues{
			PKey:   []byte(strconv.Itoa(i)),
			IndexV: int(rand.Int31n(1000)),
		}
		t.Log(respList[i])
	}
	heap.Init(&respList)
	for i := 0; i < 50; i++ {
		v := &HIndexRespWithValues{
			PKey:   []byte(strconv.Itoa(i)),
			IndexV: int(rand.Int31n(1000)),
		}
		heap.Push(&respList, v)
		t.Log(v)
	}
	var lastV int
	for respList.Len() > 0 {
		v := heap.Pop(&respList).(*HIndexRespWithValues)
		t.Log(v)
		vt, ok := v.IndexV.(int)
		assert.True(t, ok)
		t.Log(vt)
		if lastV != 0 {
			assert.True(t, vt >= lastV, "should sorted")
		}
		lastV = vt
	}
}

package metric

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopNWrite(t *testing.T) {
	topn := NewTopNHot()

	for i := 0; i < defaultTopnBucketSize; i++ {
		k := []byte(strconv.Itoa(i))
		topn.HitWrite(k)
	}
	t.Logf("%v", topn.GetTopNWrites())
	hk := []byte(strconv.Itoa(1))
	topn.HitWrite(hk)
	topn.HitWrite(hk)
	topn.HitWrite(hk)
	t.Logf("%v", topn.GetTopNWrites())
	assert.Equal(t, defaultTopnBucketSize, len(topn.GetTopNWrites()))

	for i := 0; i < defaultTopnBucketSize; i++ {
		k := []byte(strconv.Itoa(i))
		topn.HitWrite(k)
	}
	for l := 0; l < 100; l++ {
		topn.HitWrite(hk)
		topn.HitWrite(hk)
		topn.HitWrite(hk)
	}
	t.Logf("%v", topn.GetTopNWrites())
	assert.Equal(t, defaultTopnBucketSize, len(topn.GetTopNWrites()))
	keys := topn.GetTopNWrites()
	assert.Equal(t, string(hk), keys[len(keys)-1].Key)
	assert.Equal(t, true, keys[0].Cnt <= keys[1].Cnt)
	assert.Equal(t, true, keys[len(keys)-2].Cnt <= keys[len(keys)-1].Cnt)
	topn.Clear()

	assert.Equal(t, 0, len(topn.GetTopNWrites()))
}

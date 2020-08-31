package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipListOp(t *testing.T) {
	sl := NewSkipList()
	defer sl.Destroy()
	key := []byte("test")
	value := key
	v, err := sl.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, v)
	sl.Insert(key, value)
	n := sl.Size()
	assert.Equal(t, int64(1), n)
	v, err = sl.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, key, v)
	sl.Delete(key)
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()
	defer sl.Destroy()
	key := []byte("test")
	sl.Insert(key, key)
	key2 := []byte("test2")
	sl.Insert(key2, key2)
	key3 := []byte("test3")
	sl.Insert(key3, key3)
	key4 := []byte("test4")
	sl.Insert(key4, key4)
	n := sl.Size()
	assert.Equal(t, int64(4), n)
	it := sl.NewIterator()
	defer it.Close()
	it.SeekToFirst()
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	it.Prev()
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.SeekToLast()
	assert.True(t, it.Valid())
	assert.Equal(t, key4, it.Key())
	assert.Equal(t, key4, it.Value())
	it.Prev()
	assert.True(t, it.Valid())
	if !it.Valid() {
		return
	}
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	it.SeekForPrev(key3)
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	it.SeekForPrev([]byte("test1"))
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Prev()
	assert.True(t, !it.Valid())
}

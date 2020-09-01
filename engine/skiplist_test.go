package engine

import (
	"strconv"
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
	sl.Set(key, value)
	n := sl.Len()
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
	sl.Set(key, key)
	key2 := []byte("test2")
	sl.Set(key2, key2)
	key3 := []byte("test3")
	sl.Set(key3, key3)
	key4 := []byte("test4")
	sl.Set(key4, key4)
	n := sl.Len()
	assert.Equal(t, int64(4), n)
	it := sl.NewIterator()
	defer it.Close()
	it.Seek(key3)
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	it.Seek([]byte("test1"))
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())

	it.First()
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
	it.Last()
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

func TestSkipListIteratorAll(t *testing.T) {
	sl := NewSkipList()
	defer sl.Destroy()
	key := []byte("0")
	sl.Set(key, key)
	key1 := []byte("1")
	sl.Set(key1, key1)
	key2 := []byte("2")
	sl.Set(key2, key2)
	key3 := []byte("3")
	sl.Set(key3, key3)
	n := sl.Len()
	assert.Equal(t, int64(4), n)
	it := sl.NewIterator()
	defer it.Close()
	it.First()
	cnt := 0
	for ; it.Valid(); it.Next() {
		assert.Equal(t, strconv.Itoa(cnt), string(it.Key()))
		assert.Equal(t, strconv.Itoa(cnt), string(it.Value()))
		cnt++
	}
	assert.Equal(t, n, int64(cnt))
}

func TestSkipListReverseIteratorAll(t *testing.T) {
	sl := NewSkipList()
	defer sl.Destroy()
	key := []byte("0")
	sl.Set(key, key)
	key1 := []byte("1")
	sl.Set(key1, key1)
	key2 := []byte("2")
	sl.Set(key2, key2)
	key3 := []byte("3")
	sl.Set(key3, key3)
	n := sl.Len()
	assert.Equal(t, int64(4), n)
	it := sl.NewIterator()
	defer it.Close()
	it.Last()
	cnt := 0
	for ; it.Valid(); it.Prev() {
		assert.Equal(t, strconv.Itoa(int(n)-cnt-1), string(it.Key()))
		assert.Equal(t, strconv.Itoa(int(n)-cnt-1), string(it.Value()))
		cnt++
	}
	assert.Equal(t, n, int64(cnt))
}

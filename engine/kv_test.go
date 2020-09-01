package engine

import (
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func TestMain(m *testing.M) {
	SetLogger(int32(common.LOG_INFO), nil)
	flag.Parse()
	if testing.Verbose() {
		common.InitDefaultForGLogger("")
		SetLogLevel(int32(common.LOG_DETAIL))
	}
	ret := m.Run()
	os.Exit(ret)
}

func TestRocksdbCheckpointData(t *testing.T) {
	testCheckpointData(t, "rocksdb")
}

func TestPebbleEngCheckpointData(t *testing.T) {
	testCheckpointData(t, "pebble")
}

func TestMemEngBtreeCheckpointData(t *testing.T) {
	useSkiplist = false
	defer func() {
		useSkiplist = true
	}()
	testCheckpointData(t, "mem")
}
func TestMemEngSkiplistCheckpointData(t *testing.T) {
	testCheckpointData(t, "mem")
}

func testCheckpointData(t *testing.T, engType string) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	ck, err := eng.NewCheckpoint()
	assert.Nil(t, err)
	// test save should not block, so lastTs should be updated soon
	ckpath := path.Join(tmpDir, "newCk")
	os.MkdirAll(ckpath, common.DIR_PERM)
	// since the open engine will  add rocksdb as subdir, we save it to the right place
	err = ck.Save(path.Join(ckpath, engType), make(chan struct{}))
	assert.Nil(t, err)

	wb := eng.DefaultWriteBatch()
	for j := 0; j < 2; j++ {
		wb.Put([]byte("test"+strconv.Itoa(j)), []byte("test"+strconv.Itoa(j)))
	}
	eng.Write(wb)
	wb.Clear()

	ck2, err := eng.NewCheckpoint()
	assert.Nil(t, err)
	// test save should not block, so lastTs should be updated soon
	ckpath2 := path.Join(tmpDir, "newCk2")
	os.MkdirAll(ckpath2, common.DIR_PERM)
	err = ck2.Save(path.Join(ckpath2, engType), make(chan struct{}))
	assert.Nil(t, err)

	cfgCK := *cfg
	cfgCK.DataDir = ckpath
	engCK, err := NewKVEng(&cfgCK)
	assert.Nil(t, err)
	err = engCK.OpenEng()
	assert.Nil(t, err)
	defer engCK.CloseAll()

	cfgCK2 := *cfg
	cfgCK2.DataDir = ckpath2
	engCK2, err := NewKVEng(&cfgCK2)
	assert.Nil(t, err)
	err = engCK2.OpenEng()
	assert.Nil(t, err)
	defer engCK2.CloseAll()

	for j := 0; j < 2; j++ {
		key := []byte("test" + strconv.Itoa(j))
		origV, err := eng.GetBytes(key)
		assert.Equal(t, key, origV)
		v, err := engCK.GetBytes(key)
		assert.Nil(t, err)
		assert.Nil(t, v)
		v2, err := engCK2.GetBytes(key)
		assert.Nil(t, err)
		assert.Equal(t, key, v2)
		assert.Equal(t, origV, v2)
	}
	time.Sleep(time.Second)
}

func TestMemEngBtreeIterator(t *testing.T) {
	useSkiplist = false
	defer func() {
		useSkiplist = true
	}()
	testKVIterator(t, "mem")
}

func TestMemEngSkiplistIterator(t *testing.T) {
	testKVIterator(t, "mem")
}

func TestRockEngIterator(t *testing.T) {
	testKVIterator(t, "rocksdb")
}

func TestPebbleEngIterator(t *testing.T) {
	testKVIterator(t, "pebble")
}

func testKVIterator(t *testing.T, engType string) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "iterator_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	wb := eng.NewWriteBatch()
	key := []byte("test")
	wb.Put(key, key)
	eng.Write(wb)
	wb.Clear()
	key2 := []byte("test2")
	wb.Put(key2, key2)
	eng.Write(wb)
	wb.Clear()
	key3 := []byte("test3")
	wb.Put(key3, key3)
	eng.Write(wb)
	wb.Clear()
	key4 := []byte("test4")
	wb.Put(key4, key4)
	eng.Write(wb)
	wb.Clear()
	it, _ := eng.GetIterator(IteratorOpts{})
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
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	//it.SeekForPrev(key3)
	//assert.True(t, it.Valid())
	//assert.Equal(t, key2, it.Key())
	//assert.Equal(t, key2, it.Value())

	it.SeekForPrev([]byte("test5"))
	assert.True(t, it.Valid())
	assert.Equal(t, key4, it.Key())
	assert.Equal(t, key4, it.Value())

	it.SeekForPrev([]byte("test1"))
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Prev()
	assert.True(t, !it.Valid())
}

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

func TestMemEngCheckpointData(t *testing.T) {
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

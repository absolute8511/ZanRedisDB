package engine

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRockWriteAfterClose(t *testing.T) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	eng, err := NewRockEng(cfg)
	err = eng.OpenEng()
	assert.Nil(t, err)
	wb := eng.DefaultWriteBatch()
	wb.Put([]byte("test"), []byte("test"))
	wb.Put([]byte("test2"), []byte("test2"))

	eng.CloseAll()

	err = eng.Write(wb)
	assert.NotNil(t, err)
}

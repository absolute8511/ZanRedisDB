package engine

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemEngReopenAndCheck(t *testing.T) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	pe, err := NewMemEng(cfg)
	t.Logf("%s", err)
	assert.Nil(t, err)
	err = pe.OpenEng()
	t.Logf("%s", err)
	assert.Nil(t, err)

	pe.CloseAll()
	time.Sleep(time.Second * 10)
}

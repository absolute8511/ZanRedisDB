package common

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

func TestRsync(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("rsync-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	t.Log(tmpDir)
	RunFileSync("127.0.0.1", "~/test_rsync", tmpDir)
}

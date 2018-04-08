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
	RunFileSync("127.0.0.1", "~/test_rsync", tmpDir, nil)
}

//func TestHashPID(t *testing.T) {
//	// ic_event_index:18509865_335939261
//	t.Fatalf("pid: %v", int(murmur3.Sum32([]byte("ic_event_index:18510778_342808306")))%8)
//	//ic_event_index:18510778_346675939
//}

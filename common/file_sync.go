package common

import (
	"errors"
	"time"

	//"github.com/Redundancy/go-sync"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
)

var runningCh chan struct{}

func init() {
	runningCh = make(chan struct{}, 3)
}

var rsyncLimit = int64(51200)

const (
	SnapWaitTimeout = time.Minute * 20
)

var ErrTransferOutofdate = errors.New("waiting transfer snapshot too long, maybe out of date")
var ErrRsyncFailed = errors.New("transfer snapshot failed due to rsync error")

func SetRsyncLimit(limit int64) {
	atomic.StoreInt64(&rsyncLimit, limit)
}

// make sure the file sync will not overwrite hard link file inplace. (Because the hard link file content which may be
// used in rocksdb should not be changed )
// So with hard link sync, we make sure we do unlink on the file before we update it. (rsync just do it)
func RunFileSync(remote string, srcPath string, dstPath string, stopCh chan struct{}) error {
	// TODO: retrict the running number of rsync may cause transfer snapshot again and again.
	// Because there are many partitions waiting transfer, 1->2->3->4.
	// The later partition may wait too much time, in this case the snapshot maybe
	// already out of date.
	begin := time.Now()
	select {
	case runningCh <- struct{}{}:
	case <-stopCh:
		return ErrStopped
	}
	defer func() {
		select {
		case <-runningCh:
		default:
		}
	}()

	if time.Since(begin) > SnapWaitTimeout {
		return ErrTransferOutofdate
	}
	var cmd *exec.Cmd
	if filepath.Base(srcPath) == filepath.Base(dstPath) {
		dir := filepath.Dir(dstPath)
		os.MkdirAll(dir, DIR_PERM)
	} else {
		os.MkdirAll(dstPath, DIR_PERM)
	}
	if remote == "" {
		log.Printf("copy local :%v to %v\n", srcPath, dstPath)
		cmd = exec.Command("cp", "-rp", srcPath, dstPath)
	} else {
		log.Printf("copy from remote :%v/%v to local: %v\n", remote, srcPath, dstPath)
		// limit rate in kilobytes
		limitStr := fmt.Sprintf("--bwlimit=%v", atomic.LoadInt64(&rsyncLimit))
		cmd = exec.Command("rsync", "--timeout=300", "-avP", "--delete", limitStr,
			"rsync://"+remote+"/"+srcPath, dstPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	err := cmd.Run()
	if err != nil {
		log.Printf("cmd %v error: %v", cmd, err)
		return ErrRsyncFailed
	}
	return err
}

package common

import (
	//"github.com/Redundancy/go-sync"
	"log"
	"os/exec"
	"time"
)

// startStats prints the stats every statsInterval
//
// It returns a channel which should be closed to stop the stats.
func startStats() chan struct{} {
	stopStats := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-ticker.C:
				// TODO: print stats
			case <-stopStats:
				ticker.Stop()
				return
			}
		}
	}()
	return stopStats
}

// Run the function with stats and retries if required
func Run(retries int, f func() error) error {
	var err error
	stopStats := startStats()
	for try := 1; try <= retries; try++ {
		err = f()
		if err == nil {
			if try > 1 {
				log.Printf("Attempt %d/%d succeeded\n", try, retries)
			}
			break
		}
		//if fs.IsFatalError(err) {
		//	fs.ErrorLog(nil, "Fatal error received - not attempting retries")
		//	break
		//}
		//if fs.IsNoRetryError(err) {
		//	fs.ErrorLog(nil, "Can't retry this error - not attempting retries")
		//	break
		//}
		if err != nil {
			log.Printf("Attempt %d/%d failed with errors : %v\n", try, retries, err)
		}
	}
	close(stopStats)
	if err != nil {
		log.Printf("sync files Failed : %v\n", err)
		return err
	}
	return nil
}

func RunFileSync(remote string, srcPath string, dstPath string) error {
	var cmd *exec.Cmd
	if remote == "" {
		log.Printf("copy local :%v to %v\n", srcPath, dstPath)
		cmd = exec.Command("cp", "-rp", srcPath, dstPath)
	} else {
		// TODO: we need do ssh without password on the cluster nodes
		log.Printf("copy from remote \n")
		cmd = exec.Command("scp", "-rp", "-l", "409600", remote+":"+srcPath, dstPath)
	}
	err := cmd.Run()
	if err != nil {
		log.Printf("cmd error: %v\n", err)
	}
	return err
	//fs := &gosync.BasicSummary{}
	//rsync, err := gosync.MakeRSync(srcPath, remote, dstPath, fs)
	//if err != nil {
	//	return err
	//}
	//err = rsync.Patch()
	//return rsync.Close()
}

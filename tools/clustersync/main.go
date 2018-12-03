package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/youzan/ZanRedisDB/common"
)

var ip = flag.String("ip", "127.0.0.1", "server ip")
var port = flag.Int("port", 6380, "server port")
var task = flag.String("t", "", "check task type supported: check-sync-normal|check-sync-init")
var namespace = flag.String("namespace", "", "the namespace")
var stopWriteTs = flag.Int64("stopts", 0, "stop timestamps for check write syncer, the timestamps should be checked while actually stop")

type RaftProgress struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
	State string `json:"state"`
}

// raft status in raft can not marshal/unmarshal correctly, we redefine it
type CustomRaftStatus struct {
	ID             uint64                  `json:"id,omitempty"`
	Term           uint64                  `json:"term,omitempty"`
	Vote           uint64                  `json:"vote"`
	Commit         uint64                  `json:"commit"`
	Lead           uint64                  `json:"lead"`
	RaftState      string                  `json:"raft_state"`
	Applied        uint64                  `json:"applied"`
	Progress       map[uint64]RaftProgress `json:"progress,omitempty"`
	LeadTransferee uint64                  `json:"lead_transferee"`
}

type LogSyncStats struct {
	SyncRecvLatency *common.WriteStats          `json:"sync_net_latency"`
	SyncAllLatency  *common.WriteStats          `json:"sync_all_latency"`
	LogReceived     []common.LogSyncStats       `json:"log_received,omitempty"`
	LogSynced       []common.LogSyncStats       `json:"log_synced,omitempty"`
	LeaderRaftStats map[string]CustomRaftStatus `json:"leader_raft_stats,omitempty"`
}

func JsonString(v interface{}) string {
	d, _ := json.MarshalIndent(v, "", " ")
	return string(d)
}

func checkLogSync(ss LogSyncStats) (int64, bool) {
	checkOK := true
	maxTs := int64(0)
	if len(ss.LogSynced) != len(ss.LeaderRaftStats) {
		log.Printf("namespace partitions not match: %v, %v\n", len(ss.LeaderRaftStats), len(ss.LogSynced))
		checkOK = false
	}
	for _, logsynced := range ss.LogSynced {
		leaderRS, ok := ss.LeaderRaftStats[logsynced.Name]
		if !ok {
			log.Printf("namespace missing in syncer leader: %v\n", JsonString(logsynced))
			checkOK = false
			continue
		}
		delete(ss.LeaderRaftStats, logsynced.Name)
		if leaderRS.Term != logsynced.Term || leaderRS.Commit != logsynced.Index {
			log.Printf("namespace %v not synced with leader: %v, %v\n", logsynced.Name, JsonString(logsynced), JsonString(leaderRS))
			checkOK = false
			continue
		}
		if logsynced.Timestamp > maxTs {
			maxTs = logsynced.Timestamp
		}
		log.Printf("namespace %v synced(%v-%v-%v)\n", logsynced.Name, logsynced.Term, logsynced.Index, logsynced.Timestamp)
	}
	if maxTs <= *stopWriteTs {
		log.Printf("get log sync stats timestamps old %v, %v\n", maxTs, *stopWriteTs)
		checkOK = false
	}
	if len(ss.LeaderRaftStats) > 0 {
		log.Printf("namespace partitions not match: %v\n", ss.LeaderRaftStats)
		checkOK = false
	}
	return maxTs, checkOK
}

func getLogSyncStats() (LogSyncStats, error) {
	url := fmt.Sprintf("http://%s:%v/logsync/stats", *ip, *port)
	var ss LogSyncStats
	code, err := common.APIRequest("GET", url, nil, time.Second, &ss)
	if err != nil {
		log.Printf("get log sync stats err %v\n", err)
		return ss, err
	}
	if code != http.StatusOK {
		log.Printf("get log sync stats err code %v\n", code)
		return ss, errors.New("error code for request")
	}
	return ss, nil
}

func main() {
	flag.Parse()

	ss, err := getLogSyncStats()
	if err != nil {
		return
	}

	checkOK := true
	switch *task {
	case "check-sync-normal":
		syncTs, ok := checkLogSync(ss)
		if !ok {
			checkOK = false
			break
		}
		// try get log syncer again to check if any timestamps changed.
		ss, err := getLogSyncStats()
		if err != nil {
			log.Printf("get log sync stats err %v\n", err)
			checkOK = false
			return
		}
		syncTs2, ok := checkLogSync(ss)
		if !ok {
			checkOK = false
			break
		}

		if syncTs2 != syncTs {
			log.Printf("log sync stats timestamps changed %v, %v\n", syncTs, syncTs2)
			checkOK = false
			break
		}
	case "check-sync-init":
		syncTs, ok := checkLogSync(ss)
		if !ok {
			checkOK = false
			break
		}

		// try get log syncer again to check if any timestamps changed.
		ss, err := getLogSyncStats()
		if err != nil {
			log.Printf("get log sync stats err %v\n", err)
			checkOK = false
			return
		}

		syncTs2, ok := checkLogSync(ss)
		if !ok {
			checkOK = false
			break
		}

		if syncTs2 != syncTs {
			log.Printf("log sync stats timestamps changed %v, %v\n", syncTs, syncTs2)
			checkOK = false
			break
		}
	default:
		log.Printf("unknown task %v\n", *task)
		return
	}
	if !checkOK {
		return
	}
	log.Printf("all check are ok, cluster synced!!\n")
}

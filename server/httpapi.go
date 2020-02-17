package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/rockredis"

	"github.com/julienschmidt/httprouter"
	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
)

var allowStaleRead int32

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

func (crs *CustomRaftStatus) Init(s raft.Status) {
	crs.ID = s.ID
	crs.Term = s.Term
	crs.Vote = s.Vote
	crs.Commit = s.Commit
	crs.Lead = s.Lead
	crs.RaftState = s.RaftState.String()
	crs.Applied = s.Applied
	crs.Progress = make(map[uint64]RaftProgress, len(s.Progress))
	for i, pr := range s.Progress {
		var cpr RaftProgress
		cpr.Match = pr.Match
		cpr.Next = pr.Next
		cpr.State = pr.State.String()
		crs.Progress[i] = cpr
	}
	crs.LeadTransferee = s.LeadTransferee
}

type RaftStatus struct {
	LeaderInfo *common.MemberInfo   `json:"leader_info,omitempty"`
	Members    []*common.MemberInfo `json:"members,omitempty"`
	Learners   []*common.MemberInfo `json:"learners,omitempty"`
	RaftStat   CustomRaftStatus     `json:"raft_stat,omitempty"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *Server) getKey(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	key := req.RequestURI
	ns, realKey, err := common.ExtractNamesapce([]byte(key))
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	kv, err := s.GetNamespace(ns, realKey)
	if err != nil || !kv.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: err.Error()}
	}
	if v, err := kv.Node.Lookup(realKey); err == nil {
		if v == nil {
			v = []byte("")
		}
		return v, nil
	} else {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: err.Error()}
	}
}

func (s *Server) doOptimize(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	table := ps.ByName("table")
	s.OptimizeDB(ns, table)
	return nil, nil
}

func (s *Server) doBackup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	s.nsMgr.BackupDB(ns)
	return nil, nil
}

func (s *Server) doBackupAll(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.nsMgr.BackupDB("")
	return nil, nil
}

func (s *Server) doOptimizeAll(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.OptimizeDB("", "")
	return nil, nil
}

func (s *Server) doDeleteRange(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	table := ps.ByName("table")
	if ns == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "namespace should not be empty"}
	}
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	sLog.Infof("got delete range: %v from remote: %v", string(data), req.RemoteAddr)
	var dtr node.DeleteTableRange
	err = json.Unmarshal(data, &dtr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	if table != "" {
		dtr.Table = table
	}
	if err := dtr.CheckValid(); err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	err = s.DeleteRange(ns, dtr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doForceNewCluster(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	if v.Node.GetLeadMember() != nil {
		return nil, common.HttpErr{Code: http.StatusForbidden, Text: "can not force new cluster while leader is ok"}
	}

	err := s.RestartAsStandalone(ns)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doForceCleanRaftNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}

	if v.Node.IsLead() {
		return nil, common.HttpErr{Code: http.StatusForbidden, Text: "leader of raft can not be force clean"}
	}
	v.Destroy()
	return nil, nil
}

func (s *Server) doRemoveNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	sLog.Infof("got remove node request: %v from remote: %v", string(data), req.RemoteAddr)
	var m common.MemberInfo
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	nsNode := s.GetNamespaceFromFullName(m.GroupName)
	if nsNode == nil || !nsNode.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: node.ErrNamespacePartitionNotFound.Error()}
	}
	err = nsNode.Node.ProposeRemoveMember(m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doAddNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	sLog.Infof("got add node request: %v from remote: %v", string(data), req.RemoteAddr)

	var m common.MemberInfo
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	if s.dataCoord != nil {
		removing, err := s.dataCoord.IsRemovingMember(m)
		if err != nil {
			return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
		}
		if removing {
			sLog.Infof("refuse to add removing node: %v from remote: %v", m, req.RemoteAddr)
			return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "removing node should not add to cluster"}
		}
	}
	nsNode := s.GetNamespaceFromFullName(m.GroupName)
	if nsNode == nil || !nsNode.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: node.ErrNamespacePartitionNotFound.Error()}
	}
	err = nsNode.Node.ProposeAddMember(m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}

	return nil, nil
}

func (s *Server) doAddLearner(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	sLog.Infof("got add learner node request: %v from remote: %v", string(data), req.RemoteAddr)

	var m common.MemberInfo
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}

	nsNode := s.GetNamespaceFromFullName(m.GroupName)
	if nsNode == nil || !nsNode.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: node.ErrNamespacePartitionNotFound.Error()}
	}
	err = nsNode.Node.ProposeAddLearner(m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) getLeader(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	l := v.Node.GetLeadMember()
	if l == nil {
		return nil, common.HttpErr{Code: http.StatusSeeOther, Text: "no leader found"}
	}
	return l, nil
}

func (s *Server) getMembers(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	return v.Node.GetMembers(), nil
}

func (s *Server) getIndexes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		sLog.Infof("failed to get namespace node - %s", ns)
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	table := ps.ByName("table")
	return v.Node.GetIndexSchema(table)
}

func (s *Server) checkNodeAllReady(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ok := s.nsMgr.IsAllRecoveryDone()
	if !ok {
		return nil, common.HttpErr{Code: http.StatusNotAcceptable, Text: "not ready for all"}
	}
	return nil, nil
}

func (s *Server) isNsNodeFullReady(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		sLog.Infof("failed to get namespace node - %s", ns)
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	ok := v.IsNsNodeFullReady(true)
	if !ok {
		return nil, common.HttpErr{Code: http.StatusNotAcceptable, Text: "raft node is not synced yet"}
	}
	return nil, nil
}

func (s *Server) checkNodeBackup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := s.GetNamespaceFromFullName(ns)
	if v == nil || !v.IsReady() {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	meta, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	ok, err := v.Node.CheckLocalBackup(meta)
	if err != nil || !ok {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no backup found"}
	}
	return nil, nil
}

func (s *Server) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *Server) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "BAD_LEVEL_STRING"}
	}
	mode := reqParams.Get("logmode")
	switch mode {
	case "":
		sLog.SetLevel(int32(level))
		rafthttp.SetLogLevel(level)
		node.SetLogLevel(level)
		cluster.SetLogLevel(level)
		rockredis.SetLogLevel(int32(level))
	case "server":
		sLog.SetLevel(int32(level))
	case "node":
		node.SetLogLevel(level)
	case "cluster":
		cluster.SetLogLevel(level)
	case "db":
		rockredis.SetLogLevel(int32(level))
	case "rafthttp":
		rafthttp.SetLogLevel(level)
	default:
		sLog.Infof("unknown log mode: %v, available(server,node,cluster,db,rafthttp)", mode)
	}
	return nil, nil
}

func (s *Server) doSetCostLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("level")
	if levelStr == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "BAD_LEVEL_STRING"}
	}
	atomic.StoreInt32(&costStatsLevel, int32(level))
	return nil, nil
}

func (s *Server) doSetRsyncLimit(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	limitStr := reqParams.Get("limit")
	if limitStr == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "MISSING_ARG"}
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "BAD_ARG_STRING"}
	}
	if limit <= 0 {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "BAD_ARG_STRING"}
	}
	common.SetRsyncLimit(int64(limit))
	return nil, nil
}

func (s *Server) doSetStaleRead(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	allowStr := reqParams.Get("allow")
	if allowStr == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "MISSING_ARG"}
	}
	if allowStr == "true" {
		atomic.StoreInt32(&allowStaleRead, int32(1))
	} else {
		atomic.StoreInt32(&allowStaleRead, int32(0))
	}
	return nil, nil
}

func (s *Server) doSetSyncerOnly(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	param := reqParams.Get("enable")
	if param == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "MISSING_ARG"}
	}
	sLog.Infof("syncer only state changed to : %v", param)
	if param == "true" {
		node.SetSyncerOnly(true)
	} else {
		node.SetSyncerOnly(false)
	}
	return nil, nil
}

func (s *Server) doSetDynamicConf(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	paramT := reqParams.Get("type")
	paramKey := reqParams.Get("key")
	paramV := reqParams.Get("value")
	if paramT == "int" {
		n, err := strconv.Atoi(paramV)
		if err != nil {
			return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_ARG"}
		}
		common.SetIntDynamicConf(paramKey, n)
	} else if paramT == "str" {
		common.SetStrDynamicConf(paramKey, paramV)
	} else {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_ARG: param type should be int/str"}
	}
	sLog.Infof("conf %v changed to : %v", paramKey, paramV)
	return nil, nil
}

func (s *Server) doGetDynamicConf(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	paramT := reqParams.Get("type")
	paramKey := reqParams.Get("key")
	if paramT == "int" {
		v := common.GetIntDynamicConf(paramKey)
		return struct {
			Key   string `json:"key"`
			Value int    `json:"value"`
		}{
			Key:   paramKey,
			Value: v,
		}, nil
	} else if paramT == "str" {
		v := common.GetStrDynamicConf(paramKey)
		return struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{
			Key:   paramKey,
			Value: v,
		}, nil
	} else {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_ARG: param type should be int/str"}
	}
	return nil, nil
}

func (s *Server) doSetDBOptions(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	paramKey := reqParams.Get("key")
	paramV := reqParams.Get("value")
	if paramKey == "" || paramV == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_ARG: option key empty"}
	}
	sLog.Infof("try set db option %v to : %v", paramKey, paramV)
	err = s.nsMgr.SetDBOptions(paramKey, paramV)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doSetSyncerIndex(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	fromCluster := ps.ByName("clustername")
	if fromCluster == "" {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "cluster name needed"}
	}
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	var ss []common.LogSyncStats
	err = json.Unmarshal(data, &ss)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	for _, sync := range ss {
		v := s.GetNamespaceFromFullName(sync.Name)
		if v == nil || !v.IsReady() {
			continue
		}
		v.Node.SetRemoteClusterSyncedRaft(fromCluster, sync.Term, sync.Index, sync.Timestamp)
		sLog.Infof("set syncer index to: %v ", sync)
	}
	return nil, nil
}

func (s *Server) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}
	return struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		RedisPort        int    `json:"redis_port"`
		StartTime        int64  `json:"start_time"`
	}{
		Version:          common.VerBinary,
		BroadcastAddress: s.conf.BroadcastAddr,
		Hostname:         hostname,
		HTTPPort:         s.conf.HttpAPIPort,
		RedisPort:        s.conf.RedisAPIPort,
		StartTime:        s.startTime.Unix(),
	}, nil
}

func (s *Server) doRaftStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	ns := reqParams.Get("namespace")
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)
	nsList := s.nsMgr.GetNamespaces()
	rstat := make([]*RaftStatus, 0)
	for name, nsNode := range nsList {
		if !strings.HasPrefix(name, ns) {
			continue
		}
		if !nsNode.IsReady() {
			continue
		}
		if leaderOnly && !nsNode.Node.IsLead() {
			continue
		}
		var s RaftStatus
		s.LeaderInfo = nsNode.Node.GetLeadMember()
		s.Members = nsNode.Node.GetMembers()
		s.Learners = nsNode.Node.GetLearners()
		rs := nsNode.Node.GetRaftStatus()
		s.RaftStat.Init(rs)
		rstat = append(rstat, &s)
	}
	return rstat, nil
}

func (s *Server) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)
	if leaderOnlyStr == "" {
		leaderOnly = true
	}
	ss := s.GetStats(leaderOnly)

	startTime := s.startTime
	uptime := time.Since(startTime)

	return struct {
		Version string             `json:"version"`
		UpTime  int64              `json:"up_time"`
		Stats   common.ServerStats `json:"stats"`
	}{common.VerBinary, int64(uptime.Seconds()), ss}, nil
}

func (s *Server) doTableStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)
	if leaderOnlyStr == "" {
		leaderOnly = true
	}
	table := reqParams.Get("table")
	if len(table) == 0 {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST: table is needed"}
	}
	ss := s.GetTableStats(leaderOnly, table)

	return struct {
		TableStats map[string]common.TableStats `json:"table_stats"`
	}{ss}, nil
}

func (s *Server) doLogSyncStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if common.IsRoleLogSyncer(s.conf.LearnerRole) {
		allUrls := make(map[string]bool)
		recvLatency, syncLatency := node.GetLogLatencyStats()
		recvStats, syncStats := s.GetLogSyncStatsInSyncLearner()
		// note: it may happen one namespace is still waiting init, so
		// this uninit namespace sync stats may be ignored in any stats.
		for _, stat := range recvStats {
			ninfos, err := s.dataCoord.GetSnapshotSyncInfo(stat.Name)
			if err != nil {
				sLog.Infof("failed to get %v nodes info - %s", stat.Name, err)
				continue
			}
			for _, n := range ninfos {
				uri := fmt.Sprintf("http://%s:%s/raft/stats?leader_only=true",
					n.RemoteAddr, n.HttpAPIPort)
				allUrls[uri] = true
			}
		}
		allRaftStats := make(map[string]CustomRaftStatus)
		for uri, _ := range allUrls {
			rstat := make([]*RaftStatus, 0)
			sc, err := common.APIRequest("GET", uri, nil, time.Second*3, &rstat)
			if err != nil {
				sLog.Infof("request %v error: %v", uri, err)
				continue
			}
			if sc != http.StatusOK {
				sLog.Infof("request %v error: %v", uri, sc)
				continue
			}

			for _, rs := range rstat {
				if rs.LeaderInfo != nil && rs.RaftStat.RaftState == raft.StateLeader.String() {
					allRaftStats[rs.LeaderInfo.GroupName] = rs.RaftStat
				}
			}
		}
		// get leader raft log stats
		return struct {
			SyncRecvLatency *common.WriteStats          `json:"sync_net_latency"`
			SyncAllLatency  *common.WriteStats          `json:"sync_all_latency"`
			LogReceived     []common.LogSyncStats       `json:"log_received,omitempty"`
			LogSynced       []common.LogSyncStats       `json:"log_synced,omitempty"`
			LeaderRaftStats map[string]CustomRaftStatus `json:"leader_raft_stats,omitempty"`
		}{recvLatency, syncLatency, recvStats, syncStats, allRaftStats}, nil
	}
	netStat := syncClusterNetStats.Copy()
	totalStat := syncClusterTotalStats.Copy()
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)

	if leaderOnlyStr == "" {
		leaderOnly = true
	}
	logSyncedStats := s.GetLogSyncStats(leaderOnly, reqParams.Get("cluster"))
	return struct {
		SyncNetLatency *common.WriteStats    `json:"sync_net_latency"`
		SyncAllLatency *common.WriteStats    `json:"sync_all_latency"`
		LogSynced      []common.LogSyncStats `json:"log_synced,omitempty"`
	}{netStat, totalStat, logSyncedStats}, nil
}

func (s *Server) doDBStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)

	if leaderOnlyStr == "" {
		leaderOnly = true
	}
	ss := s.GetDBStats(leaderOnly)
	return ss, nil
}

func (s *Server) doWALDBStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	leaderOnlyStr := reqParams.Get("leader_only")
	leaderOnly, _ := strconv.ParseBool(leaderOnlyStr)

	if leaderOnlyStr == "" {
		leaderOnly = true
	}
	ss := s.GetWALDBStats(leaderOnly)
	return ss, nil
}

func (s *Server) doDBPerf(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("level")
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: "INVALID_REQUEST"}
	}

	node.SetPerfLevel(level)
	sLog.Infof("perf level set to: %v", level)
	return nil, nil
}

func setBlockRateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetBlockProfileRate(rate)
	return nil, nil
}

func setMutexProfileHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetMutexProfileFraction(rate)
	return nil, nil
}

func (s *Server) initHttpHandler() {
	log := common.HttpLog(sLog, common.LOG_INFO)
	debugLog := common.HttpLog(sLog, common.LOG_DEBUG)
	router := httprouter.New()
	router.Handle("GET", common.APIGetLeader+"/:namespace", common.Decorate(s.getLeader, common.V1))
	router.Handle("GET", common.APIGetMembers+"/:namespace", common.Decorate(s.getMembers, common.V1))
	router.Handle("GET", common.APIGetIndexes+"/:namespace/:table", common.Decorate(s.getIndexes, common.V1))
	router.Handle("GET", common.APIGetIndexes+"/:namespace", common.Decorate(s.getIndexes, common.V1))
	router.Handle("GET", common.APICheckBackup+"/:namespace", common.Decorate(s.checkNodeBackup, log, common.V1))
	router.Handle("GET", common.APIIsRaftSynced+"/:namespace", common.Decorate(s.isNsNodeFullReady, common.V1))
	router.Handle("GET", "/kv/get/:namespace", common.Decorate(s.getKey, common.PlainText))
	router.Handle("POST", "/kv/optimize/:namespace/:table", common.Decorate(s.doOptimize, log, common.V1))
	router.Handle("POST", "/kv/optimize", common.Decorate(s.doOptimizeAll, log, common.V1))
	router.Handle("POST", "/kv/backup/:namespace", common.Decorate(s.doBackup, log, common.V1))
	router.Handle("POST", "/kv/backup/", common.Decorate(s.doBackupAll, log, common.V1))
	router.Handle("POST", "/cluster/raft/forcenew/:namespace", common.Decorate(s.doForceNewCluster, log, common.V1))
	router.Handle("POST", "/cluster/raft/forceclean/:namespace", common.Decorate(s.doForceCleanRaftNode, log, common.V1))
	router.Handle("POST", common.APIAddNode, common.Decorate(s.doAddNode, log, common.V1))
	router.Handle("POST", common.APIAddLearnerNode, common.Decorate(s.doAddLearner, log, common.V1))
	router.Handle("POST", common.APIRemoveNode, common.Decorate(s.doRemoveNode, log, common.V1))
	router.Handle("GET", common.APINodeAllReady, common.Decorate(s.checkNodeAllReady, common.V1))
	router.Handle("POST", "/kv/delrange/:namespace/:table", common.Decorate(s.doDeleteRange, log, common.V1))

	router.Handle("GET", "/ping", common.Decorate(s.pingHandler, common.PlainText))
	router.Handle("POST", "/loglevel/set", common.Decorate(s.doSetLogLevel, log, common.V1))
	router.Handle("POST", "/costlevel/set", common.Decorate(s.doSetCostLevel, log, common.V1))
	router.Handle("POST", "/rsynclimit", common.Decorate(s.doSetRsyncLimit, log, common.V1))
	router.Handle("POST", "/staleread", common.Decorate(s.doSetStaleRead, log, common.V1))
	router.Handle("POST", "/synceronly", common.Decorate(s.doSetSyncerOnly, log, common.V1))
	router.Handle("POST", "/conf/set", common.Decorate(s.doSetDynamicConf, log, common.V1))
	router.Handle("GET", "/conf/get", common.Decorate(s.doGetDynamicConf, log, common.V1))
	router.Handle("GET", "/info", common.Decorate(s.doInfo, common.V1))
	router.Handle("POST", "/syncer/setindex/:clustername", common.Decorate(s.doSetSyncerIndex, log, common.V1))

	router.Handle("GET", "/stats", common.Decorate(s.doStats, common.V1))
	router.Handle("GET", common.APITableStats, common.Decorate(s.doTableStats, common.V1))
	router.Handle("GET", "/logsync/stats", common.Decorate(s.doLogSyncStats, common.V1))
	router.Handle("GET", "/db/stats", common.Decorate(s.doDBStats, common.V1))
	router.Handle("POST", "/db/options/set", common.Decorate(s.doSetDBOptions, log, common.V1))
	router.Handle("GET", "/waldb/stats", common.Decorate(s.doWALDBStats, common.V1))
	router.Handle("GET", "/db/perf", common.Decorate(s.doDBPerf, log, common.V1))
	router.Handle("GET", "/raft/stats", common.Decorate(s.doRaftStats, debugLog, common.V1))

	router.Handle("POST", "/debug/setblockrate", common.Decorate(setBlockRateHandler, log, common.V1))
	router.Handle("POST", "/debug/setmutexrate", common.Decorate(setMutexProfileHandler, log, common.V1))
	s.router = router
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func (s *Server) serveHttpAPI(port int, stopC <-chan struct{}) {
	s.initHttpHandler()
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: s,
	}
	l, err := common.NewStoppableListener(srv.Addr, stopC)
	if err != nil {
		panic(err)
	}
	err = srv.Serve(l)
	// exit when raft goes down
	sLog.Infof("http server stopped: %v", err)
}

package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/julienschmidt/httprouter"
)

func (self *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func (self *Server) getKey(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	key := req.RequestURI
	ns, realKey, err := common.ExtractNamesapce([]byte(key))
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	kv := self.GetNamespace(ns)
	if kv == nil {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "Namespace not found:" + ns}
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

func (self *Server) doOptimize(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	self.OptimizeDB()
	return nil, nil
}

func (self *Server) doAddNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	sLog.Infof("got add node request: %v", string(data))

	var m node.MemberInfo
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
	}
	nsNode := self.GetNamespace(m.GroupName)
	if nsNode == nil {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: errNamespaceNotFound.Error()}
	}
	err = nsNode.Node.ProposeAddMember(m)
	if err != nil {
		return nil, common.HttpErr{Code: http.StatusInternalServerError, Text: err.Error()}
	}

	return nil, nil
}

func (self *Server) getLeader(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	l := v.Node.GetLeadMember()
	if l == nil {
		return nil, common.HttpErr{Code: http.StatusSeeOther, Text: "no leader found"}
	}
	return l, nil
}

func (self *Server) getMembers(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
		return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	return v.Node.GetMembers(), nil
}

func (self *Server) checkNodeBackup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
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

func (self *Server) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (self *Server) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, common.HttpErr{400, "BAD_LEVEL_STRING"}
	}
	sLog.SetLevel(int32(level))
	rafthttp.SetLogLevel(level)
	node.SetLogLevel(level)
	cluster.SetLogLevel(level)
	return nil, nil
}

func (self *Server) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
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
		BroadcastAddress: self.conf.BroadcastAddr,
		Hostname:         hostname,
		HTTPPort:         self.conf.HttpAPIPort,
		RedisPort:        self.conf.RedisAPIPort,
		StartTime:        self.startTime.Unix(),
	}, nil
}

func (self *Server) doRaftStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}
	ns := reqParams.Get("namespace")
	return self.dataCoord.Stats(ns, -1), nil
}

func (self *Server) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		sLog.Infof("failed to parse request params - %s", err)
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}
	_ = reqParams.Get("namespace")
	leaderOnlyStr := reqParams.Get("leader_only")
	_, _ = strconv.ParseBool(leaderOnlyStr)

	startTime := self.startTime
	uptime := time.Since(startTime)

	return struct {
		Version string `json:"version"`
		UpTime  int64  `json:"up_time"`
	}{common.VerBinary, int64(uptime.Seconds())}, nil
}

func (self *Server) initHttpHandler() {
	log := common.HttpLog(sLog)
	//debugLog := common.HttpLog(common.LOG_DEBUG, sLog.Logger)
	router := httprouter.New()
	router.Handle("GET", common.APIGetLeader, common.Decorate(self.getLeader, common.V1))
	router.Handle("GET", common.APIGetMembers, common.Decorate(self.getMembers, common.V1))
	router.Handle("GET", common.APICheckBackup, common.Decorate(self.checkNodeBackup, common.V1))
	router.Handle("GET", "/kv/get/:namespace", common.Decorate(self.getKey, common.PlainText))
	router.Handle("POST", "/kv/optimize", common.Decorate(self.doOptimize, log, common.V1))
	router.Handle("POST", common.APIAddNode, common.Decorate(self.doAddNode, log, common.V1))

	router.Handle("GET", "/ping", common.Decorate(self.pingHandler, common.PlainText))
	router.Handle("POST", "/loglevel/set", common.Decorate(self.doSetLogLevel, log, common.V1))
	router.Handle("GET", "/info", common.Decorate(self.doInfo, common.V1))

	router.Handle("GET", "/stats", common.Decorate(self.doStats, common.V1))
	router.Handle("GET", "/raft/stats", common.Decorate(self.doRaftStats, common.V1))

	self.router = router
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func (self *Server) serveHttpAPI(port int, stopC <-chan struct{}) {
	go http.ListenAndServe(":6666", nil)
	self.initHttpHandler()
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: self,
	}
	l, err := common.NewStoppableListener(srv.Addr, stopC)
	if err != nil {
		panic(err)
	}
	err = srv.Serve(l)
	// exit when raft goes down
	sLog.Infof("http server stopped: %v", err)
}

package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/julienschmidt/httprouter"
)

func (self *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func (self *Server) getKey(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	key := req.RequestURI
	ns, realKey, err := common.ExtractNamesapce([]byte(key))
	if err != nil {
		return nil, Err{Code: http.StatusBadRequest, Text: err.Error()}
	}
	kv, ok := self.kvNodes[ns]
	if !ok || kv == nil {
		return nil, Err{Code: http.StatusNotFound, Text: "Namespace not found:" + ns}
	}
	if v, err := kv.node.Lookup(realKey); err == nil {
		if v == nil {
			v = []byte("")
		}
		return v, nil
	} else {
		return nil, Err{Code: http.StatusNotFound, Text: err.Error()}
	}
}

func (self *Server) doOptimize(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	self.OptimizeDB()
	return nil, nil
}

func (self *Server) doAddNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, Err{Code: http.StatusBadRequest, Text: err.Error()}
	}

	var m node.MemberInfo
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, Err{Code: http.StatusBadRequest, Text: err.Error()}
	}
	data, _ = json.Marshal(m)

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  m.ID,
		Context: data,
	}
	self.ProposeConfChange(m.Namespace, cc)

	return nil, nil
}

func (self *Server) doRemoveNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	nodeIdStr := ps.ByName("node")
	if len(nodeIdStr) == 0 {
		return nil, Err{Code: http.StatusBadRequest, Text: "missing node id"}
	}
	nodeId, err := strconv.ParseUint(nodeIdStr, 10, 64)
	if err != nil {
		return nil, Err{Code: http.StatusBadRequest, Text: err.Error()}
	}
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	self.ProposeConfChange(ns, cc)
	return nil, nil
}

func (self *Server) getLeader(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
		return nil, Err{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	l := v.node.GetLeadMember()
	if l == nil {
		return nil, Err{Code: http.StatusSeeOther, Text: "no leader found"}
	}
	return l, nil
}

func (self *Server) getMembers(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
		return nil, Err{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	return v.node.GetMembers(), nil
}

func (self *Server) checkNodeBackup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	v := self.GetNamespace(ns)
	if v == nil {
		return nil, Err{Code: http.StatusNotFound, Text: "no namespace found"}
	}
	meta, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, Err{Code: http.StatusBadRequest, Text: err.Error()}
	}
	ok, err := v.node.CheckLocalBackup(meta)
	if err != nil || !ok {
		return nil, Err{Code: http.StatusNotFound, Text: "no backup found"}
	}
	return nil, nil
}

func (self *Server) initHttpHandler() {
	log := Log(1)
	router := httprouter.New()
	router.Handle("GET", "/cluster/leader/:namespace", Decorate(self.getLeader, V1))
	router.Handle("GET", "/cluster/members/:namespace", Decorate(self.getMembers, V1))
	router.Handle("GET", "/cluster/checkbackup/:namespace", Decorate(self.checkNodeBackup, V1))
	router.Handle("GET", "/kv/get/:namespace", Decorate(self.getKey, PlainText))
	router.Handle("POST", "/kv/optimize", Decorate(self.doOptimize, log, V1))
	router.Handle("POST", "/cluster/node/add", Decorate(self.doAddNode, log, V1))
	router.Handle("DELETE", "/cluster/node/remove/:namespace/:node", Decorate(self.doRemoveNode, log, V1))
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

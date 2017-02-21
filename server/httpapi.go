package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
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

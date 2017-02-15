package pdserver

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/julienschmidt/httprouter"
)

func (self *Server) initHttpHandler() {
	//log := common.HttpLog(common.LOG_INFO, sLog.Logger)
	router := httprouter.New()
	router.Handle("GET", "/cluster/leader/:namespace", common.Decorate(self.getLeader, common.V1))
	self.router = router
}

func (self *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func (self *Server) getLeader(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	//v := self.GetNamespace(ns)
	//if v == nil {
	//	return nil, common.HttpErr{Code: http.StatusNotFound, Text: "no namespace found"}
	//}
	//l := v.Node.GetLeadMember()
	//if l == nil {
	//	return nil, common.HttpErr{Code: http.StatusSeeOther, Text: "no leader found"}
	//}
	//return l, nil
	return ns, nil
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func (self *Server) serveHttpAPI(addr string, stopC <-chan struct{}) {
	go http.ListenAndServe(":6667", nil)
	self.initHttpHandler()
	srv := http.Server{
		Addr:    addr,
		Handler: self,
	}
	l, err := common.NewStoppableListener(srv.Addr, stopC)
	if err != nil {
		panic(err)
	}
	err = srv.Serve(l)
	sLog.Infof("http server stopped: %v", err)
}

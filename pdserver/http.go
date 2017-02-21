package pdserver

import (
	"errors"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"strconv"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/julienschmidt/httprouter"
)

type node struct {
	BroadcastAddress string `json:"broadcast_address"`
	Hostname         string `json:"hostname"`
	RedisPort        string `json:"redis_port"`
	HTTPPort         string `json:"http_port"`
	Version          string `json:"version"`
}

func GetValidPartitionNum(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= common.MAX_PARTITION_NUM {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_NUM")
}

func GetValidPartitionID(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num >= 0 && num < common.MAX_PARTITION_NUM {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_ID")
}

func GetValidReplicator(r string) (int, error) {
	num, err := strconv.Atoi(r)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= common.MAX_REPLICATOR {
		return num, nil
	}
	return 0, errors.New("INVALID_REPLICATOR")
}

func (self *Server) initHttpHandler() {
	log := common.HttpLog(sLog)
	router := httprouter.New()
	router.Handle("GET", "/ping", common.Decorate(self.pingHandler, common.PlainText))
	router.Handle("GET", "/info", common.Decorate(self.getInfo, common.V1))
	router.Handle("GET", "/namespaces", common.Decorate(self.getNamespaces, common.V1))
	router.Handle("GET", "/datanodes", common.Decorate(self.getDataNodes, common.V1))
	router.Handle("GET", "/listpd", common.Decorate(self.listPDNodes, common.V1))
	router.Handle("GET", "/query/:namespace", common.Decorate(self.doQueryNamespace, common.V1))

	// cluster prefix url means only handled by leader of pd
	router.Handle("GET", "/cluster/stats", common.Decorate(self.doClusterStats, common.V1))
	router.Handle("POST", "/cluster/pd/tombstone", common.Decorate(self.doClusterTombstonePD, log, common.V1))
	router.Handle("POST", "/cluster/node/remove", common.Decorate(self.doClusterRemoveDataNode, log, common.V1))
	router.Handle("POST", "/cluster/upgrade/begin", common.Decorate(self.doClusterBeginUpgrade, log, common.V1))
	router.Handle("POST", "/cluster/upgrade/done", common.Decorate(self.doClusterFinishUpgrade, log, common.V1))
	router.Handle("POST", "/cluster/namespace/create", common.Decorate(self.doCreateNamespace, log, common.V1))
	router.Handle("POST", "/cluster/namespace/delete", common.Decorate(self.doDeleteNamespace, log, common.V1))

	router.Handle("POST", "/loglevel/set", common.Decorate(self.doSetLogLevel, log, common.V1))
	self.router = router
}

func (self *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func (self *Server) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (self *Server) getInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: common.VerBinary,
	}, nil
}

func (self *Server) getNamespaces(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	namespaces, _, err := self.pdCoord.GetAllNamespaces()
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	nameList := make([]string, 0, len(namespaces))
	for ns, _ := range namespaces {
		nameList = append(nameList, ns)
	}
	return map[string]interface{}{
		"namespaces": nameList,
	}, nil
}

func (self *Server) getDataNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	nodes := make([]*node, 0)
	dns, _ := self.pdCoord.GetAllDataNodes()
	for _, n := range dns {
		dn := &node{
			BroadcastAddress: n.NodeIP,
			Hostname:         n.Hostname,
			Version:          n.Version,
			RedisPort:        n.RedisPort,
			HTTPPort:         n.HttpPort,
		}
		nodes = append(nodes, dn)
	}
	return map[string]interface{}{
		"nodes": nodes,
	}, nil
}

func (self *Server) listPDNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	nodes, err := self.pdCoord.GetAllPDNodes()
	if err != nil {
		sLog.Infof("list error: %v", err)
		return nil, common.HttpErr{500, err.Error()}
	}
	filteredNodes := nodes[:0]
	for _, n := range nodes {
		if !self.IsTombstonePDNode(n.GetID()) {
			filteredNodes = append(filteredNodes, n)
		}
	}
	leader := self.pdCoord.GetPDLeader()
	return map[string]interface{}{
		"pdnodes":  filteredNodes,
		"pdleader": leader,
	}, nil
}

func (self *Server) doQueryNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	if ns == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_NAMESPACE"}
	}

	namespaces, _, err := self.pdCoord.GetAllNamespaces()
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	nsInfo, ok := namespaces[ns]
	if !ok {
		return nil, common.HttpErr{404, "NAMESPACE not found"}
	}
	// TODO: for each partition, get the node info (ip, port)

	return nsInfo, nil
}

func (self *Server) doClusterStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	stable := false
	if !self.pdCoord.IsMineLeader() {
		sLog.Infof("request from remote %v should request to leader", req.RemoteAddr)
		return nil, common.HttpErr{400, cluster.ErrFailedOnNotLeader}
	}
	stable = self.pdCoord.IsClusterStable()

	return struct {
		Stable bool `json:"stable"`
	}{
		Stable: stable,
	}, nil
}

func (self *Server) doClusterTombstonePD(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}

	node := reqParams.Get("node")
	if node == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_NODE"}
	}
	restore := reqParams.Get("restore")
	if restore != "" {
		deleted := self.DelTombstonePDNode(node)
		if deleted {
			return nil, nil
		} else {
			return nil, common.HttpErr{404, "node id not found"}
		}
	}
	nodes, err := self.pdCoord.GetAllPDNodes()
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	var peer cluster.NodeInfo
	for _, n := range nodes {
		if n.GetID() == node {
			peer.ID = n.GetID()
			peer.NodeIP = n.NodeIP
			break
		}
	}
	if peer.GetID() == "" {
		return nil, common.HttpErr{404, "node id not found"}
	} else {
		self.TombstonePDNode(peer.GetID(), peer)
	}
	return nil, nil
}

func (self *Server) doClusterRemoveDataNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}
	nid := reqParams.Get("remove_node")

	err = self.pdCoord.MarkNodeAsRemoving(nid)
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	return nil, nil
}

func (self *Server) doClusterBeginUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	err := self.pdCoord.SetClusterUpgradeState(true)
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	return nil, nil
}

func (self *Server) doClusterFinishUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	err := self.pdCoord.SetClusterUpgradeState(false)
	if err != nil {
		return nil, common.HttpErr{500, err.Error()}
	}
	return nil, nil
}

func (self *Server) doCreateNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}

	ns := reqParams.Get("namespace")
	if ns == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_NAMESPACE"}
	}

	if !common.IsValidNamespaceName(ns) {
		return nil, common.HttpErr{400, "INVALID_ARG_NAMESPACE"}
	}
	engType := reqParams.Get("engtype")
	if engType == "" {
		engType = "rocksdb"
	}

	pnumStr := reqParams.Get("partition_num")
	if pnumStr == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_PARTITION_NUM"}
	}
	pnum, err := GetValidPartitionNum(pnumStr)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_ARG_PARTITION_NUM"}
	}
	replicatorStr := reqParams.Get("replicator")
	if replicatorStr == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_REPLICATOR"}
	}
	replicator, err := GetValidReplicator(replicatorStr)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_ARG_REPLICATOR"}
	}

	if !self.pdCoord.IsMineLeader() {
		return nil, common.HttpErr{400, cluster.ErrFailedOnNotLeader}
	}
	var meta cluster.NamespaceMetaInfo
	meta.PartitionNum = pnum
	meta.Replica = replicator
	meta.EngType = engType

	err = self.pdCoord.CreateNamespace(ns, meta)
	if err != nil {
		sLog.Infof("create namespace failed: %v, %v", ns, err)
		return nil, common.HttpErr{500, err.Error()}
	}
	return nil, nil
}

func (self *Server) doDeleteNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{400, "INVALID_REQUEST"}
	}

	name := reqParams.Get("namespace")
	if name == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_NAMESPACE"}
	}
	partStr := reqParams.Get("partition")
	if partStr == "" {
		return nil, common.HttpErr{400, "MISSING_ARG_PARTITION"}
	} else if partStr == "**" {
		sLog.Warningf("removing all the partitions of : %v", name)
	} else {
		return nil, common.HttpErr{400, "REMOVE_SINGLE_PARTITION_NOT_ALLOWED"}
	}

	sLog.Infof("deleting (%s) with partition %v ", name, partStr)
	err = self.pdCoord.DeleteNamespace(name, partStr)
	if err != nil {
		sLog.Infof("deleting (%s) with partition %v failed : %v", name, partStr, err)
		return nil, common.HttpErr{500, err.Error()}
	}
	return nil, nil
}

func (self *Server) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return nil, nil
}

func (self *Server) IsTombstonePDNode(nid string) bool {
	self.dataMutex.Lock()
	_, ok := self.tombstonePDNodes[nid]
	self.dataMutex.Unlock()
	return ok
}

func (self *Server) TombstonePDNode(nid string, n cluster.NodeInfo) {
	self.dataMutex.Lock()
	self.tombstonePDNodes[nid] = true
	self.dataMutex.Unlock()
}

func (self *Server) DelTombstonePDNode(nid string) bool {
	self.dataMutex.Lock()
	_, ok := self.tombstonePDNodes[nid]
	if ok {
		delete(self.tombstonePDNodes, nid)
	}
	self.dataMutex.Unlock()
	return ok
}

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

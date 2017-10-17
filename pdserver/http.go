package pdserver

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"strconv"
	"strings"

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
	DCInfo           string `json:"dc_info"`
}

type PartitionNodeInfo struct {
	Leader   node   `json:"leader"`
	Replicas []node `json:"replicas"`
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

func (s *Server) initHttpHandler() {
	log := common.HttpLog(sLog, common.LOG_INFO)
	debugLog := common.HttpLog(sLog, common.LOG_DEBUG)
	router := httprouter.New()
	router.Handle("GET", "/ping", common.Decorate(s.pingHandler, common.PlainText))
	router.Handle("GET", "/info", common.Decorate(s.getInfo, common.V1))
	router.Handle("GET", "/namespaces", common.Decorate(s.getNamespaces, common.V1))
	router.Handle("GET", "/datanodes", common.Decorate(s.getDataNodes, common.V1))
	router.Handle("GET", "/listpd", common.Decorate(s.listPDNodes, common.V1))
	router.Handle("GET", "/query/:namespace", common.Decorate(s.doQueryNamespace, debugLog, common.V1))

	// cluster prefix url means only handled by leader of pd
	router.Handle("GET", "/cluster/stats", common.Decorate(s.doClusterStats, common.V1))
	router.Handle("POST", "/cluster/pd/tombstone", common.Decorate(s.doClusterTombstonePD, log, common.V1))
	router.Handle("POST", "/cluster/node/remove", common.Decorate(s.doClusterRemoveDataNode, log, common.V1))
	router.Handle("POST", "/cluster/upgrade/begin", common.Decorate(s.doClusterBeginUpgrade, log, common.V1))
	router.Handle("POST", "/cluster/upgrade/done", common.Decorate(s.doClusterFinishUpgrade, log, common.V1))
	router.Handle("POST", "/cluster/namespace/create", common.Decorate(s.doCreateNamespace, log, common.V1))
	router.Handle("DELETE", "/cluster/namespace/delete", common.Decorate(s.doDeleteNamespace, log, common.V1))
	router.Handle("POST", "/cluster/schema/index/add", common.Decorate(s.doAddIndexSchema, log, common.V1))
	router.Handle("DELETE", "/cluster/schema/index/del", common.Decorate(s.doDelIndexSchema, log, common.V1))
	router.Handle("POST", "/cluster/namespace/meta/update", common.Decorate(s.doUpdateNamespaceMeta, log, common.V1))
	router.Handle("POST", "/stable/nodenum", common.Decorate(s.doSetStableNodeNum, log, common.V1))

	router.Handle("POST", "/loglevel/set", common.Decorate(s.doSetLogLevel, log, common.V1))
	s.router = router
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *Server) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *Server) getInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: common.VerBinary,
	}, nil
}

func (s *Server) getNamespaces(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	namespaces, _, err := s.pdCoord.GetAllNamespaces()
	if err != nil {
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	nameList := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		nameList = append(nameList, ns)
	}
	return map[string]interface{}{
		"namespaces": nameList,
	}, nil
}

func (s *Server) getDataNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	nodes := make([]*node, 0)
	dns, epoch := s.pdCoord.GetAllDataNodes()
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
	if len(nodes) == 0 {
		sLog.Infof("no data nodes found: %v, %v", dns, epoch)
	}
	return map[string]interface{}{
		"nodes": nodes,
	}, nil
}

func (s *Server) listPDNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	nodes, err := s.pdCoord.GetAllPDNodes()
	if err != nil {
		sLog.Infof("list error: %v", err)
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	filteredNodes := nodes[:0]
	for _, n := range nodes {
		if !s.IsTombstonePDNode(n.GetID()) {
			filteredNodes = append(filteredNodes, n)
		}
	}
	leader := s.pdCoord.GetPDLeader()
	return map[string]interface{}{
		"pdnodes":  filteredNodes,
		"pdleader": leader,
	}, nil
}

func (s *Server) doQueryNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	ns := ps.ByName("namespace")
	if ns == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}
	epoch := reqParams.Get("epoch")
	disableCache := reqParams.Get("disable_cache")

	namespaces, curEpoch, err := s.pdCoord.GetAllNamespaces()
	if err != nil {
		if len(namespaces) == 0 || disableCache == "true" {
			return nil, common.HttpErr{Code: 500, Text: err.Error()}
		}
		sLog.Infof("get namespaces error, using cached data %v", curEpoch)
	}
	nsPartsInfo, ok := namespaces[ns]
	if !ok {
		return nil, common.HttpErr{Code: 404, Text: "NAMESPACE not found"}
	}
	if epoch == strconv.FormatInt(curEpoch, 10) {
		return nil, common.HttpErr{Code: 304, Text: "cluster namespaces unchanged"}
	}
	dns, _ := s.pdCoord.GetAllDataNodes()
	partNodes := make(map[int]PartitionNodeInfo)

	pnum := 0
	engType := ""
	for _, nsInfo := range nsPartsInfo {
		pnum = nsInfo.PartitionNum
		engType = nsInfo.EngType
		var pn PartitionNodeInfo
		for _, nid := range nsInfo.RaftNodes {
			n, ok := dns[nid]
			ip, _, redisPort, httpPort := cluster.ExtractNodeInfoFromID(nid)
			hostname := ""
			version := ""
			dcInfo := ""
			if ok {
				hostname = n.Hostname
				version = n.Version
				dc, ok := n.Tags[cluster.DCInfoTag]
				if ok {
					dcInfo, ok = dc.(string)
				}
			}
			dn := node{
				BroadcastAddress: ip,
				Hostname:         hostname,
				Version:          version,
				RedisPort:        redisPort,
				HTTPPort:         httpPort,
				DCInfo:           dcInfo,
			}
			if nsInfo.GetRealLeader() == nid {
				pn.Leader = dn
			}
			pn.Replicas = append(pn.Replicas, dn)
		}
		partNodes[nsInfo.Partition] = pn
	}
	return map[string]interface{}{
		"epoch":         curEpoch,
		"partition_num": pnum,
		"eng_type":      engType,
		"partitions":    partNodes,
	}, nil
}

func (s *Server) doClusterStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	stable := false
	if !s.pdCoord.IsMineLeader() {
		sLog.Infof("request from remote %v should request to leader", req.RemoteAddr)
		return nil, common.HttpErr{Code: 400, Text: cluster.ErrFailedOnNotLeader}
	}
	stable = s.pdCoord.IsClusterStable()

	return struct {
		Stable bool `json:"stable"`
	}{
		Stable: stable,
	}, nil
}

func (s *Server) doClusterTombstonePD(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	node := reqParams.Get("node")
	if node == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NODE"}
	}
	restore := reqParams.Get("restore")
	if restore != "" {
		deleted := s.DelTombstonePDNode(node)
		if deleted {
			return nil, nil
		} else {
			return nil, common.HttpErr{Code: 404, Text: "node id not found"}
		}
	}
	nodes, err := s.pdCoord.GetAllPDNodes()
	if err != nil {
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
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
		return nil, common.HttpErr{Code: 404, Text: "node id not found"}
	} else {
		s.TombstonePDNode(peer.GetID(), peer)
	}
	return nil, nil
}

func (s *Server) doClusterRemoveDataNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}
	nid := reqParams.Get("remove_node")

	err = s.pdCoord.MarkNodeAsRemoving(nid)
	if err != nil {
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doClusterBeginUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	err := s.pdCoord.SetClusterUpgradeState(true)
	if err != nil {
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doClusterFinishUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	err := s.pdCoord.SetClusterUpgradeState(false)
	if err != nil {
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doCreateNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	ns := reqParams.Get("namespace")
	if ns == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}

	if !common.IsValidNamespaceName(ns) {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_NAMESPACE"}
	}
	engType := reqParams.Get("engtype")
	if engType == "" {
		engType = "rockredis"
	}

	pnumStr := reqParams.Get("partition_num")
	if pnumStr == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_PARTITION_NUM"}
	}
	pnum, err := GetValidPartitionNum(pnumStr)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_PARTITION_NUM"}
	}
	replicatorStr := reqParams.Get("replicator")
	if replicatorStr == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_REPLICATOR"}
	}
	replicator, err := GetValidReplicator(replicatorStr)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_REPLICATOR"}
	}

	expPolicy := reqParams.Get("expiration_policy")
	if expPolicy == "" {
		expPolicy = common.DefaultExpirationPolicy
	} else if _, err := common.StringToExpirationPolicy(expPolicy); err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_EXPIRATION_POLICY"}
	}

	tagStr := reqParams.Get("tags")
	var tagList []string
	if tagStr != "" {
		tagList = strings.Split(tagStr, ",")
	}

	if !s.pdCoord.IsMineLeader() {
		return nil, common.HttpErr{Code: 400, Text: cluster.ErrFailedOnNotLeader}
	}
	var meta cluster.NamespaceMetaInfo
	meta.PartitionNum = pnum
	meta.Replica = replicator
	meta.EngType = engType
	meta.ExpirationPolicy = expPolicy
	meta.Tags = make(map[string]interface{})
	for _, tag := range tagList {
		if strings.TrimSpace(tag) != "" {
			meta.Tags[strings.TrimSpace(tag)] = true
		}
	}

	err = s.pdCoord.CreateNamespace(ns, meta)
	if err != nil {
		sLog.Infof("create namespace failed: %v, %v", ns, err)
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doDeleteNamespace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	name := reqParams.Get("namespace")
	if name == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}
	partStr := reqParams.Get("partition")
	if partStr == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_PARTITION"}
	} else if partStr == "**" {
		sLog.Warningf("removing all the partitions of : %v", name)
	} else {
		return nil, common.HttpErr{Code: 400, Text: "REMOVE_SINGLE_PARTITION_NOT_ALLOWED"}
	}

	sLog.Infof("deleting (%s) with partition %v ", name, partStr)
	err = s.pdCoord.DeleteNamespace(name, partStr)
	if err != nil {
		sLog.Infof("deleting (%s) with partition %v failed : %v", name, partStr, err)
		return nil, common.HttpErr{Code: 500, Text: err.Error()}
	}
	return nil, nil
}

func (s *Server) doAddIndexSchema(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	ns := reqParams.Get("namespace")
	if ns == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}

	if !common.IsValidNamespaceName(ns) {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_NAMESPACE"}
	}
	table := reqParams.Get("table")

	if table == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_TABLE_NAME"}
	}
	if !s.pdCoord.IsMineLeader() {
		return nil, common.HttpErr{Code: 400, Text: cluster.ErrFailedOnNotLeader}
	}

	indexType := reqParams.Get("indextype")
	if indexType == "" {
		sLog.Infof("missing index type: %v, %v", ns, table)
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_INDEX_TYPE"}
	}
	if indexType == "hash_secondary" {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			sLog.Infof("read schema body error: %v, %v, %v", ns, table, err)
			return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
		}
		var meta common.HsetIndexSchema
		err = json.Unmarshal(data, &meta)
		if err != nil {
			sLog.Infof("schema body unmarshal error: %v, %v, %v", ns, table, err)
			return nil, common.HttpErr{Code: http.StatusBadRequest, Text: err.Error()}
		}
		sLog.Infof("add hash index : %v, %v", ns, meta)
		err = s.pdCoord.AddHIndexSchema(ns, table, &meta)
		if err != nil {
			sLog.Infof("add hash index failed: %v, %v", ns, err)
			return nil, common.HttpErr{Code: 500, Text: err.Error()}
		}
	} else if indexType == "json_secondary" {
		return nil, common.HttpErr{Code: 400, Text: "unsupported index type"}
	} else {
		return nil, common.HttpErr{Code: 400, Text: "unsupported index type"}
	}

	return nil, nil
}

func (s *Server) doDelIndexSchema(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	ns := reqParams.Get("namespace")
	if ns == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}

	if !common.IsValidNamespaceName(ns) {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_NAMESPACE"}
	}
	table := reqParams.Get("table")

	if table == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_TABLE_NAME"}
	}
	if !s.pdCoord.IsMineLeader() {
		return nil, common.HttpErr{Code: 400, Text: cluster.ErrFailedOnNotLeader}
	}

	indexType := reqParams.Get("indextype")
	if indexType == "" {
		sLog.Infof("del index missing type: %v", ns)
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_INDEX_TYPE"}
	}
	indexName := reqParams.Get("indexname")
	if indexName == "" {
		sLog.Infof("del index missing name: %v", ns)
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_INDEX_NAME"}
	}

	if indexType == "hash_secondary" {
		sLog.Infof("del hash index : %v, %v", ns, indexName)
		err = s.pdCoord.DelHIndexSchema(ns, table, indexName)
		if err != nil {
			sLog.Infof("del hash index failed: %v, %v", ns, err)
			return nil, common.HttpErr{Code: 500, Text: err.Error()}
		}
	} else if indexType == "json_secondary" {
		return nil, common.HttpErr{Code: 400, Text: "unsupported index type"}
	} else {
		return nil, common.HttpErr{Code: 400, Text: "unsupported index type"}
	}

	return nil, nil
}

func (s *Server) doUpdateNamespaceMeta(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	ns := reqParams.Get("namespace")
	if ns == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NAMESPACE"}
	}

	if !common.IsValidNamespaceName(ns) {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_NAMESPACE"}
	}

	replicatorStr := reqParams.Get("replicator")
	replicator := -1
	if replicatorStr != "" {
		replicator, err = GetValidReplicator(replicatorStr)
		if err != nil {
			return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_REPLICATOR"}
		}
	}

	optimizeFsyncStr := reqParams.Get("optimizefsync")
	snapStr := reqParams.Get("snapcount")
	snapCount := -1
	if snapStr != "" {
		snapCount, err = strconv.Atoi(snapStr)
		if err != nil {
			return nil, common.HttpErr{Code: 400, Text: "INVALID_ARG_SNAP"}
		}
	}

	if !s.pdCoord.IsMineLeader() {
		return nil, common.HttpErr{Code: 400, Text: cluster.ErrFailedOnNotLeader}
	}
	err = s.pdCoord.ChangeNamespaceMetaParam(ns, replicator, optimizeFsyncStr, snapCount)
	if err != nil {
		sLog.Infof("update namespace meta failed: %v, %v", ns, err)
		return nil, common.HttpErr{Code: 400, Text: err.Error()}
	}
	return nil, nil

}

func (s *Server) doSetStableNodeNum(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}

	numStr := reqParams.Get("number")
	if numStr == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_NUMBER"}
	}
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "BAD_ARG_STRING"}
	}
	err = s.pdCoord.SetClusterStableNodeNum(num)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: err.Error()}
	}

	return nil, nil
}

func (s *Server) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, common.HttpErr{Code: 400, Text: "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, common.HttpErr{Code: 400, Text: "BAD_LEVEL_STRING"}
	}
	sLog.SetLevel(int32(level))
	cluster.SetLogLevel(level)
	return nil, nil
}

func (s *Server) IsTombstonePDNode(nid string) bool {
	s.dataMutex.Lock()
	_, ok := s.tombstonePDNodes[nid]
	s.dataMutex.Unlock()
	return ok
}

func (s *Server) TombstonePDNode(nid string, n cluster.NodeInfo) {
	s.dataMutex.Lock()
	s.tombstonePDNodes[nid] = true
	s.dataMutex.Unlock()
}

func (s *Server) DelTombstonePDNode(nid string) bool {
	s.dataMutex.Lock()
	_, ok := s.tombstonePDNodes[nid]
	if ok {
		delete(s.tombstonePDNodes, nid)
	}
	s.dataMutex.Unlock()
	return ok
}

func (s *Server) serveHttpAPI(addr string, stopC <-chan struct{}) {
	if s.conf.ProfilePort != "" {
		go http.ListenAndServe(":"+s.conf.ProfilePort, nil)
	}
	s.initHttpHandler()
	srv := http.Server{
		Addr:    addr,
		Handler: s,
	}
	l, err := common.NewStoppableListener(srv.Addr, stopC)
	if err != nil {
		panic(err)
	}
	err = srv.Serve(l)
	sLog.Infof("http server stopped: %v", err)
}

package server

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/store"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

var (
	errNamespaceNotFound = errors.New("namespace not found")
)

type NamespaceNode struct {
	node        *node.KVNode
	conf        *NamespaceConfig
	confChangeC chan raftpb.ConfChange
}

type Server struct {
	mutex   sync.Mutex
	kvNodes map[string]*NamespaceNode
	conf    ServerConfig
	stopC   chan struct{}
	wg      sync.WaitGroup
}

func NewServer(conf ServerConfig) *Server {
	s := &Server{
		kvNodes: make(map[string]*NamespaceNode),
		conf:    conf,
		stopC:   make(chan struct{}),
	}
	return s
}

func (self *Server) Stop() {
	self.mutex.Lock()
	for k, n := range self.kvNodes {
		n.node.Stop()
		log.Printf("kv namespace stopped: %v", k)
	}
	self.mutex.Unlock()
	close(self.stopC)
	self.wg.Wait()
	log.Printf("server stopped")
}

func (self *Server) GetStats() common.ServerStats {
	var ss common.ServerStats
	self.mutex.Lock()
	for k, n := range self.kvNodes {
		ns := n.node.GetStats()
		ns.Name = k
		ns.EngType = n.conf.EngType
		ss.NSStats = append(ss.NSStats, ns)
	}
	self.mutex.Unlock()
	return ss
}

func (self *Server) OptimizeDB() {
	self.mutex.Lock()
	nodeList := make([]*NamespaceNode, 0, len(self.kvNodes))
	for _, n := range self.kvNodes {
		nodeList = append(nodeList, n)
	}
	self.mutex.Unlock()
	for _, n := range nodeList {
		n.node.OptimizeDB()
	}
}

func (self *Server) onNamespaceDeleted(ns string) func() {
	return func() {
		self.mutex.Lock()
		_, ok := self.kvNodes[ns]
		if ok {
			log.Printf("namespace deleted: %v", ns)
			delete(self.kvNodes, ns)
		}
		self.mutex.Unlock()
	}
}

func (self *Server) InitKVNamespace(clusterID uint64, id int, localRaftAddr string,
	clusterNodes map[int]string, join bool, conf *NamespaceConfig) error {
	kvOpts := &store.KVOptions{
		DataDir: self.conf.DataDir,
		EngType: conf.EngType,
	}
	kv, confC := node.NewKVNode(kvOpts, clusterID, id, localRaftAddr,
		clusterNodes, join, self.onNamespaceDeleted(conf.Name))
	n := &NamespaceNode{
		node:        kv,
		conf:        conf,
		confChangeC: confC,
	}
	self.mutex.Lock()
	self.kvNodes[conf.Name] = n
	self.mutex.Unlock()
	return nil
}

func (self *Server) ProposeConfChange(ns string, cc raftpb.ConfChange) {
	self.mutex.Lock()
	nsNode, ok := self.kvNodes[ns]
	self.mutex.Unlock()
	if ok {
		nsNode.confChangeC <- cc
	} else {
		log.Printf("namespace not found: %v", ns)
	}
}

func (self *Server) ServeAPI() {
	// api server should disable the api request while starting until replay log finished and
	// also while we recovery we need to disable api.
	self.wg.Add(2)
	go func() {
		defer self.wg.Done()
		self.serveRedisAPI(self.conf.RedisAPIPort, self.stopC)
	}()
	go func() {
		defer self.wg.Done()
		self.serveHttpAPI(self.conf.HTTPAPIPort, self.stopC)
	}()
}

func (self *Server) GetHandler(cmdName string, cmd redcon.Command) (common.CommandFunc, redcon.Command, error) {
	if len(cmd.Args) < 2 {
		return nil, cmd, common.ErrInvalidArgs
	}
	rawKey := cmd.Args[1]

	namespace, _, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		return nil, cmd, err
	}
	self.mutex.Lock()
	n, ok := self.kvNodes[namespace]
	self.mutex.Unlock()
	if !ok || n == nil {
		return nil, cmd, errNamespaceNotFound
	}
	h, ok := n.node.GetHandler(cmdName)
	if !ok {
		return nil, cmd, common.ErrInvalidCommand
	}
	return h, cmd, nil
}

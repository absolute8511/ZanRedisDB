package server

import (
	"bytes"
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
	errInvalidRedisKey   = errors.New("invalid redis key")
	errNamespaceNotFound = errors.New("namespace not found")
)

func extractNamesapce(rawKey []byte) (string, []byte, error) {
	index := bytes.IndexByte(rawKey, ':')
	if index <= 0 {
		return "", nil, errInvalidRedisKey
	}
	namespace := string(rawKey[:index])
	realKey := rawKey[index+1:]
	return namespace, realKey, nil
}

type Server struct {
	kvNodes     map[string]*node.KVNode
	conf        ServerConfig
	stopC       chan struct{}
	wg          sync.WaitGroup
	confChangeC chan raftpb.ConfChange
}

func NewServer(conf ServerConfig) *Server {
	s := &Server{
		kvNodes:     make(map[string]*node.KVNode),
		conf:        conf,
		confChangeC: make(chan raftpb.ConfChange),
		stopC:       make(chan struct{}),
	}
	return s
}

func (self *Server) Stop() {
	for k, n := range self.kvNodes {
		n.Stop()
		log.Printf("kv namespace stopped: %v", k)
	}
	close(self.stopC)
	self.wg.Wait()
	log.Printf("server stopped")
}

func (self *Server) InitKVNamespace(clusterID uint64, id int, raftAddr string,
	clusterNodes map[int]string, join bool) error {
	kvOpts := &store.KVOptions{
		DataDir: self.conf.DataDir,
		EngType: self.conf.EngType,
	}
	kv := node.NewKVNode(kvOpts, clusterID, id, raftAddr,
		clusterNodes, join, self.confChangeC)
	self.kvNodes["default"] = kv
	return nil
}

func (self *Server) ProposeConfChange(cc raftpb.ConfChange) {
	self.confChangeC <- cc
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

	namespace, realKey, err := extractNamesapce(rawKey)
	if err != nil {
		return nil, cmd, err
	}
	cmd.Args[1] = realKey
	n, ok := self.kvNodes[namespace]
	if !ok || n == nil {
		return nil, cmd, errNamespaceNotFound
	}
	h, ok := n.GetHandler(cmdName)
	if !ok {
		return nil, cmd, common.ErrInvalidCommand
	}
	return h, cmd, nil
}

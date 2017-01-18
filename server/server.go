package server

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/store"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/tidwall/redcon"
	"golang.org/x/net/context"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"
)

var (
	errNamespaceNotFound = errors.New("namespace not found")
)

var sLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("server"))

func SetLogger(level int32, logger common.Logger) {
	sLog.SetLevel(level)
	sLog.Logger = logger
}

func SLogger() *common.LevelLogger {
	return sLog
}

type NamespaceNode struct {
	node *node.KVNode
	conf *NamespaceConfig
}

type Server struct {
	mutex         sync.Mutex
	kvNodes       map[string]*NamespaceNode
	groups        map[uint64]string
	conf          ServerConfig
	stopC         chan struct{}
	raftHttpDoneC chan struct{}
	wg            sync.WaitGroup
	router        http.Handler
	raftTransport *rafthttp.Transport
}

func NewServer(conf ServerConfig) *Server {
	s := &Server{
		kvNodes:       make(map[string]*NamespaceNode),
		groups:        make(map[uint64]string),
		conf:          conf,
		stopC:         make(chan struct{}),
		raftHttpDoneC: make(chan struct{}),
	}

	ss := &stats.ServerStats{}
	ss.Initialize()
	s.raftTransport = &rafthttp.Transport{
		DialTimeout: time.Second * 5,
		ID:          types.ID(conf.NodeID),
		ClusterID:   types.ID(conf.ClusterID),
		Raft:        s,
		Snapshotter: s,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(conf.NodeID))),
		ErrorC:      nil,
	}

	return s
}

func (self *Server) Stop() {
	self.raftTransport.Stop()
	close(self.stopC)
	<-self.raftHttpDoneC
	self.mutex.Lock()
	for k, n := range self.kvNodes {
		n.node.Stop()
		sLog.Infof("kv namespace stopped: %v", k)
	}
	self.mutex.Unlock()
	self.wg.Wait()
	sLog.Infof("server stopped")
}

func (self *Server) GetNamespace(ns string) *NamespaceNode {
	self.mutex.Lock()
	v, _ := self.kvNodes[ns]
	self.mutex.Unlock()
	return v
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

func (self *Server) onNamespaceDeleted(gid uint64, ns string) func() {
	return func() {
		self.mutex.Lock()
		_, ok := self.kvNodes[ns]
		if ok {
			sLog.Infof("namespace deleted: %v-%v", ns, gid)
			delete(self.kvNodes, ns)
			delete(self.groups, gid)
		}
		self.mutex.Unlock()
	}
}

func (self *Server) InitKVNamespace(gID uint64, id uint64, localRaftAddr string,
	clusterNodes map[uint64]node.ReplicaInfo, join bool, conf *NamespaceConfig) error {
	kvOpts := &store.KVOptions{
		DataDir: path.Join(self.conf.DataDir, conf.Name),
		EngType: conf.EngType,
	}
	nc := &node.NodeConfig{
		NodeID:        self.conf.NodeID,
		BroadcastAddr: self.conf.BroadcastAddr,
		HttpAPIPort:   self.conf.HttpAPIPort,
	}
	raftConf := &node.RaftConfig{
		GroupID:     gID,
		GroupName:   conf.Name,
		ID:          uint64(id),
		RaftAddr:    localRaftAddr,
		DataDir:     kvOpts.DataDir,
		RaftPeers:   clusterNodes,
		SnapCount:   conf.SnapCount,
		SnapCatchup: conf.SnapCatchup,
	}
	self.mutex.Lock()
	if _, ok := self.kvNodes[conf.Name]; ok {
		self.mutex.Unlock()
		return errors.New("namespace already exist")
	}
	kv := node.NewKVNode(kvOpts, nc, raftConf, self.raftTransport,
		join, self.onNamespaceDeleted(gID, conf.Name))
	n := &NamespaceNode{
		node: kv,
		conf: conf,
	}
	self.kvNodes[conf.Name] = n
	self.groups[gID] = conf.Name
	self.mutex.Unlock()
	return nil
}

func (self *Server) ProposeConfChange(ns string, cc raftpb.ConfChange) {
	self.mutex.Lock()
	nsNode, ok := self.kvNodes[ns]
	self.mutex.Unlock()
	if ok {
		nsNode.node.ProposeConfChange(cc)
	} else {
		sLog.Infof("namespace not found: %v", ns)
	}
}

func (self *Server) Start() {
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.raftTransport.Start()
		self.serveRaft()
	}()
	for _, kv := range self.kvNodes {
		kv.node.Start()
	}
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.processRaftTick()
	}()
	// api server should disable the api request while starting until replay log finished and
	// also while we recovery we need to disable api.
	self.wg.Add(2)
	go func() {
		defer self.wg.Done()
		self.serveRedisAPI(self.conf.RedisAPIPort, self.stopC)
	}()
	go func() {
		defer self.wg.Done()
		self.serveHttpAPI(self.conf.HttpAPIPort, self.stopC)
	}()
}

func (self *Server) GetHandler(cmdName string, cmd redcon.Command) (common.CommandFunc, redcon.Command, error) {
	if len(cmd.Args) < 2 {
		return nil, cmd, common.ErrInvalidArgs
	}
	rawKey := cmd.Args[1]

	namespace, _, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		sLog.Infof("failed to get the namespace of the redis command:%v", rawKey)
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

func (self *Server) processRaftTick() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// send tick for all raft group
			self.mutex.Lock()
			nodes := make([]*node.KVNode, 0, len(self.kvNodes))
			for _, v := range self.kvNodes {
				nodes = append(nodes, v.node)
			}
			self.mutex.Unlock()
			for _, n := range nodes {
				n.Tick()
			}
		case <-self.stopC:
			return
		}
	}
}

func (self *Server) serveRaft() {
	url, err := url.Parse(self.conf.LocalRaftAddr)
	if err != nil {
		sLog.Fatalf("failed parsing raft url: %v", err)
	}
	ln, err := common.NewStoppableListener(url.Host, self.stopC)
	if err != nil {
		sLog.Fatalf("failed to listen rafthttp : %v", err)
	}
	err = (&http.Server{Handler: self.raftTransport.Handler()}).Serve(ln)
	select {
	case <-self.stopC:
	default:
		sLog.Fatalf("failed to serve rafthttp : %v", err)
	}
	sLog.Infof("raft http transport exit")
	close(self.raftHttpDoneC)
}

// implement the Raft interface for transport
func (self *Server) Process(ctx context.Context, m raftpb.Message) error {
	//sLog.Infof("got message from raft transport %v ", m.String())
	self.mutex.Lock()
	gn, ok := self.groups[m.ToGroup.GroupId]
	if !ok {
		sLog.Errorf("group name not found %v ", m.ToGroup)
		gn = m.ToGroup.Name
	}
	kv, ok := self.kvNodes[gn]
	self.mutex.Unlock()
	if !ok {
		sLog.Errorf("kv namespace not found %v while processing %v ", gn, m.String())
		return errors.New("raft group not found")
	}
	return kv.node.Process(ctx, m)
}

func (self *Server) IsIDRemoved(id uint64, group raftpb.Group) bool { return false }

func (self *Server) ReportUnreachable(id uint64, group raftpb.Group) {
	//sLog.Infof("report node %v in group %v unreachable", id, group)
	self.mutex.Lock()
	gn, ok := self.groups[group.GroupId]
	if !ok {
		sLog.Errorf("group name not found %v-%v ", id, group)
		gn = group.Name
	}
	kv, ok := self.kvNodes[gn]
	self.mutex.Unlock()
	if !ok {
		sLog.Errorf("kv namespace not found %v ", gn)
		return
	}
	kv.node.ReportUnreachable(id, group)
}

func (self *Server) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	sLog.Infof("node %v in group %v snapshot status: %v", id, gp, status)
	self.mutex.Lock()
	gn, ok := self.groups[gp.GroupId]
	if !ok {
		sLog.Errorf("group name not found %v-%v ", id, gp)
		gn = gp.Name
	}
	kv, ok := self.kvNodes[gn]
	self.mutex.Unlock()
	if !ok {
		sLog.Errorf("kv namespace not found %v ", gn)
		return
	}
	kv.node.ReportSnapshot(id, gp, status)
}

// implement the snapshotter interface for transport
func (self *Server) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	self.mutex.Lock()
	gn, ok := self.groups[msg.ToGroup.GroupId]
	if !ok {
		sLog.Errorf("group name not found %v ", msg.ToGroup)
		gn = msg.ToGroup.Name
	}
	kv, ok := self.kvNodes[gn]
	self.mutex.Unlock()
	if !ok {
		sLog.Errorf("kv namespace not found %v ", gn)
		return 0, errors.New("raft group not found")
	}

	return kv.node.SaveDBFrom(r, msg)
}

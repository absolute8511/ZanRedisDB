package server

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/stats"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/coreos/etcd/pkg/types"
	"github.com/tidwall/redcon"
	"golang.org/x/net/context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	errNamespaceNotFound = errors.New("namespace not found")
	errRaftGroupNotReady = errors.New("raft group not ready")
)

var sLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("server"))

func SetLogger(level int32, logger common.Logger) {
	sLog.SetLevel(level)
	sLog.Logger = logger
}

func SLogger() *common.LevelLogger {
	return sLog
}

type Server struct {
	mutex         sync.Mutex
	conf          ServerConfig
	stopC         chan struct{}
	raftHttpDoneC chan struct{}
	wg            sync.WaitGroup
	router        http.Handler
	raftTransport *rafthttp.Transport
	dataCoord     *cluster.DataCoordinator
	nsMgr         *node.NamespaceMgr
	startTime     time.Time
}

func NewServer(conf ServerConfig) *Server {
	hname, err := os.Hostname()
	if err != nil {
		sLog.Fatal(err)
	}
	myNode := &cluster.NodeInfo{
		NodeIP:    conf.BroadcastAddr,
		Hostname:  hname,
		RedisPort: strconv.Itoa(conf.RedisAPIPort),
		HttpPort:  strconv.Itoa(conf.HttpAPIPort),
		Version:   common.VerBinary,
	}
	if conf.ClusterID == "" {
		sLog.Fatalf("cluster id can not be empty")
	}
	if conf.BroadcastInterface != "" {
		myNode.NodeIP = common.GetIPv4ForInterfaceName(conf.BroadcastInterface)
	}
	if myNode.NodeIP == "" {
		myNode.NodeIP = conf.BroadcastAddr
	} else {
		conf.BroadcastAddr = myNode.NodeIP
	}
	if myNode.NodeIP == "0.0.0.0" || myNode.NodeIP == "" {
		sLog.Fatalf("can not decide the broadcast ip: %v", myNode.NodeIP)
	}
	myNode.RaftTransportAddr = conf.LocalRaftAddr
	os.MkdirAll(conf.DataDir, common.DIR_PERM)

	s := &Server{
		conf:          conf,
		stopC:         make(chan struct{}),
		raftHttpDoneC: make(chan struct{}),
		startTime:     time.Now(),
	}

	ts := &stats.TransportStats{}
	ts.Initialize()
	s.raftTransport = &rafthttp.Transport{
		DialTimeout: time.Second * 5,
		ClusterID:   conf.ClusterID,
		Raft:        s,
		Snapshotter: s,
		TrStats:     ts,
		PeersStats:  stats.NewPeersStats(),
		ErrorC:      nil,
	}
	mconf := &node.MachineConfig{
		BroadcastAddr: conf.BroadcastAddr,
		HttpAPIPort:   conf.HttpAPIPort,
		LocalRaftAddr: conf.LocalRaftAddr,
		DataRootDir:   conf.DataDir,
	}
	s.nsMgr = node.NewNamespaceMgr(s.raftTransport, mconf)
	myNode.RegID = mconf.NodeID

	r := cluster.NewDNEtcdRegister(conf.EtcdClusterAddresses)
	s.dataCoord = cluster.NewDataCoordinator(conf.ClusterID, myNode, s.nsMgr)
	if err := s.dataCoord.SetRegister(r); err != nil {
		sLog.Fatalf("failed to init register for coordinator: %v", err)
	}
	s.raftTransport.ID = types.ID(s.dataCoord.GetMyRegID())

	return s
}

func (self *Server) Stop() {
	sLog.Infof("server begin stopping")
	self.dataCoord.Stop()
	close(self.stopC)
	self.raftTransport.Stop()
	<-self.raftHttpDoneC
	self.wg.Wait()
	sLog.Infof("server stopped")
}

func (self *Server) GetNamespace(ns string) *node.NamespaceNode {
	return self.nsMgr.GetNamespaceNode(ns)
}

func (self *Server) GetStats() common.ServerStats {
	var ss common.ServerStats
	ss.NSStats = self.nsMgr.GetStats()
	return ss
}

func (self *Server) OptimizeDB() {
	self.nsMgr.OptimizeDB()
}

func (self *Server) InitKVNamespace(id uint64, conf *node.NamespaceConfig) (*node.NamespaceNode, error) {
	return self.nsMgr.InitNamespaceNode(conf, id)
}

func (self *Server) Start() {
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.raftTransport.Start()
		self.serveRaft()
	}()

	err := self.dataCoord.Start()
	if err != nil {
		sLog.Fatalf("data coordinator start failed: %v", err)
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
	// we need decide the partition id from the primary key
	n := self.nsMgr.GetNamespaceNode(namespace)
	if n == nil {
		return nil, cmd, errNamespaceNotFound
	}
	h, ok := n.Node.GetHandler(cmdName)
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
			self.nsMgr.ProcessRaftTick()
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
	kv := self.nsMgr.GetNamespaceNodeFromGID(m.ToGroup.GroupId)
	if kv == nil {
		sLog.Errorf("kv namespace not found while processing %v ", m.String())
		return errNamespaceNotFound
	}
	if !kv.IsReady() {
		return errRaftGroupNotReady
	}
	return kv.Node.Process(ctx, m)
}

func (self *Server) IsIDRemoved(id uint64, group raftpb.Group) bool { return false }

func (self *Server) ReportUnreachable(id uint64, group raftpb.Group) {
	//sLog.Infof("report node %v in group %v unreachable", id, group)
	kv := self.nsMgr.GetNamespaceNodeFromGID(group.GroupId)
	if kv == nil {
		sLog.Errorf("kv namespace not found %v ", group.GroupId)
		return
	}
	if !kv.IsReady() {
		return
	}
	kv.Node.ReportUnreachable(id, group)
}

func (self *Server) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	sLog.Infof("node %v in group %v snapshot status: %v", id, gp, status)
	kv := self.nsMgr.GetNamespaceNodeFromGID(gp.GroupId)
	if kv == nil {
		sLog.Errorf("kv namespace not found %v ", gp.GroupId)
		return
	}
	if !kv.IsReady() {
		return
	}

	kv.Node.ReportSnapshot(id, gp, status)
}

// implement the snapshotter interface for transport
func (self *Server) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	kv := self.nsMgr.GetNamespaceNodeFromGID(msg.ToGroup.GroupId)
	if kv == nil {
		return 0, errNamespaceNotFound
	}
	if !kv.IsReady() {
		return 0, errRaftGroupNotReady
	}

	return kv.Node.SaveDBFrom(r, msg)
}

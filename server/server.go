package server

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/cluster/datanode_coord"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/raft"
	"github.com/absolute8511/ZanRedisDB/raft/raftpb"
	"github.com/absolute8511/ZanRedisDB/stats"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/absolute8511/redcon"
	"github.com/coreos/etcd/pkg/types"
	"golang.org/x/net/context"
)

var (
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
	dataCoord     *datanode_coord.DataCoordinator
	nsMgr         *node.NamespaceMgr
	startTime     time.Time
	maxScanJob    int32
	scanStats     common.ScanStats
}

func NewServer(conf ServerConfig) *Server {
	hname, err := os.Hostname()
	if err != nil {
		sLog.Fatal(err)
	}
	if conf.TickMs < 100 {
		conf.TickMs = 100
	}
	if conf.ElectionTick < 5 {
		conf.ElectionTick = 5
	}
	if conf.MaxScanJob <= 0 {
		conf.MaxScanJob = common.MAX_SCAN_JOB
	}
	if conf.ProfilePort == 0 {
		conf.ProfilePort = 7666
	}
	myNode := &cluster.NodeInfo{
		NodeIP:      conf.BroadcastAddr,
		Hostname:    hname,
		RedisPort:   strconv.Itoa(conf.RedisAPIPort),
		HttpPort:    strconv.Itoa(conf.HttpAPIPort),
		Version:     common.VerBinary,
		Tags:        make(map[string]bool),
		DataRoot:    conf.DataDir,
		RsyncModule: "zanredisdb",
	}
	if conf.DataRsyncModule != "" {
		myNode.RsyncModule = conf.DataRsyncModule
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
	conf.LocalRaftAddr = strings.Replace(conf.LocalRaftAddr, "0.0.0.0", myNode.NodeIP, 1)
	myNode.RaftTransportAddr = conf.LocalRaftAddr
	for _, tag := range conf.Tags {
		myNode.Tags[tag] = true
	}
	os.MkdirAll(conf.DataDir, common.DIR_PERM)

	s := &Server{
		conf:          conf,
		stopC:         make(chan struct{}),
		raftHttpDoneC: make(chan struct{}),
		startTime:     time.Now(),
		maxScanJob:    conf.MaxScanJob,
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
		TickMs:        conf.TickMs,
		ElectionTick:  conf.ElectionTick,
		RocksDBOpts:   conf.RocksDBOpts,
	}
	s.nsMgr = node.NewNamespaceMgr(s.raftTransport, mconf)
	myNode.RegID = mconf.NodeID

	if conf.EtcdClusterAddresses != "" {
		r := cluster.NewDNEtcdRegister(conf.EtcdClusterAddresses)
		s.dataCoord = datanode_coord.NewDataCoordinator(conf.ClusterID, myNode, s.nsMgr)
		if err := s.dataCoord.SetRegister(r); err != nil {
			sLog.Fatalf("failed to init register for coordinator: %v", err)
		}
		s.raftTransport.ID = types.ID(s.dataCoord.GetMyRegID())
		s.nsMgr.SetClusterInfoInterface(s.dataCoord)
	} else {
		s.raftTransport.ID = types.ID(myNode.RegID)
	}

	return s
}

func (self *Server) Stop() {
	sLog.Infof("server begin stopping")
	if self.dataCoord != nil {
		self.dataCoord.Stop()
	} else {
		self.nsMgr.Stop()
	}
	close(self.stopC)
	self.raftTransport.Stop()
	<-self.raftHttpDoneC
	self.wg.Wait()
	sLog.Infof("server stopped")
}

func (self *Server) GetNamespace(ns string, pk []byte) (*node.NamespaceNode, error) {
	return self.nsMgr.GetNamespaceNodeWithPrimaryKey(ns, pk)
}
func (self *Server) GetNamespaceFromFullName(ns string) *node.NamespaceNode {
	return self.nsMgr.GetNamespaceNode(ns)
}

func (self *Server) GetStats(leaderOnly bool) common.ServerStats {
	var ss common.ServerStats
	ss.NSStats = self.nsMgr.GetStats(leaderOnly)
	ss.ScanStats = self.scanStats.Copy()
	return ss
}

func (self *Server) GetDBStats(leaderOnly bool) map[string]string {
	return self.nsMgr.GetDBStats(leaderOnly)
}

func (self *Server) OptimizeDB() {
	self.nsMgr.OptimizeDB()
}

func (self *Server) InitKVNamespace(id uint64, conf *node.NamespaceConfig, join bool) (*node.NamespaceNode, error) {
	return self.nsMgr.InitNamespaceNode(conf, id, join)
}

func (self *Server) RestartAsStandalone(fullNamespace string) error {
	if self.dataCoord != nil {
		return self.dataCoord.RestartAsStandalone(fullNamespace)
	}
	return nil
}

func (self *Server) Start() {
	self.raftTransport.Start()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.serveRaft()
	}()

	if self.dataCoord != nil {
		err := self.dataCoord.Start()
		if err != nil {
			sLog.Fatalf("data coordinator start failed: %v", err)
		}
	} else {
		self.nsMgr.Start()
	}

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

	namespace, pk, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		sLog.Infof("failed to get the namespace of the redis command:%v", string(rawKey))
		return nil, cmd, err
	}
	// we need decide the partition id from the primary key
	// if the command need cross multi partitions, we need handle separate
	n, err := self.nsMgr.GetNamespaceNodeWithPrimaryKey(namespace, pk)
	if err != nil {
		return nil, cmd, err
	}
	// TODO: for multi primary keys such as mset, mget, we need make sure they are all in the same partition
	h, isWrite, ok := n.Node.GetHandler(cmdName)
	if !ok {
		return nil, cmd, common.ErrInvalidCommand
	}
	if !isWrite && !n.Node.IsLead() {
		// read only to leader to avoid stale read
		// TODO: also read command can request the raft read index if not leader
		return nil, cmd, node.ErrNamespaceNotLeader
	}
	return h, cmd, nil
}

func (self *Server) GetMergeHandlers(cmd redcon.Command) ([]common.MergeCommandFunc, []redcon.Command, error) {
	if len(cmd.Args) < 2 {
		return nil, nil, common.ErrInvalidArgs
	}
	rawKey := cmd.Args[1]

	namespace, realKey, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		sLog.Infof("failed to get the namespace of the redis command:%v", string(rawKey))
		return nil, nil, err
	}

	nodes, err := self.nsMgr.GetNamespaceNodes(namespace, true)
	if err != nil {
		return nil, nil, err
	}

	cmdName := strings.ToLower(string(cmd.Args[0]))
	var cmds map[string]redcon.Command
	//do nodes filter
	if common.IsMergeCommand(cmdName) {
		cmds, err = self.doScanNodesFilter(realKey, namespace, cmd, nodes)
		if err != nil {
			return nil, nil, err
		}
	} else {
		cmds = make(map[string]redcon.Command)
		for k, _ := range nodes {
			newCmd := common.DeepCopyCmd(cmd)
			cmds[k] = newCmd
		}
	}

	var handlers []common.MergeCommandFunc
	var commands []redcon.Command
	for k, v := range nodes {
		newCmd := cmds[k]
		h, ok := v.Node.GetMergeHandler(cmdName)
		if ok {
			handlers = append(handlers, h)
			commands = append(commands, newCmd)
		}
	}

	if len(handlers) <= 0 {
		return nil, nil, common.ErrInvalidCommand
	}

	return handlers, commands, nil
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
	if m.Type == raftpb.MsgVoteResp {
		sLog.Infof("got message from raft transport %v ", m.String())
	}
	kv := self.nsMgr.GetNamespaceNodeFromGID(m.ToGroup.GroupId)
	if kv == nil {
		sLog.Errorf("kv namespace not found while processing %v ", m.String())
		return node.ErrNamespacePartitionNotFound
	}
	if !kv.IsReady() {
		return errRaftGroupNotReady
	}
	return kv.Node.Process(ctx, m)
}

func (self *Server) IsPeerRemoved(peerID uint64) bool { return false }

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
		return 0, node.ErrNamespacePartitionNotFound
	}
	if !kv.IsReady() {
		return 0, errRaftGroupNotReady
	}

	return kv.Node.SaveDBFrom(r, msg)
}

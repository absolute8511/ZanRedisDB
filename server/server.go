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
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/rockredis"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/cluster/datanode_coord"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/stats"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
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
		conf.MaxScanJob = int32(common.MAX_SCAN_JOB)
	}
	if conf.ProfilePort == 0 {
		conf.ProfilePort = 7666
	}

	if conf.SyncerWriteOnly {
		node.SetSyncerOnly(true)
	}
	if conf.LearnerRole != "" && conf.SyncerNormalInit {
		sLog.Infof("server started as normal init")
		node.SetSyncerNormalInit()
	}

	myNode := &cluster.NodeInfo{
		NodeIP:      conf.BroadcastAddr,
		Hostname:    hname,
		RedisPort:   strconv.Itoa(conf.RedisAPIPort),
		HttpPort:    strconv.Itoa(conf.HttpAPIPort),
		RpcPort:     strconv.Itoa(conf.GrpcAPIPort),
		Version:     common.VerBinary,
		Tags:        make(map[string]interface{}),
		DataRoot:    conf.DataDir,
		RsyncModule: "zanredisdb",
		LearnerRole: conf.LearnerRole,
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
	if conf.RsyncLimit > 0 {
		common.SetRsyncLimit(conf.RsyncLimit)
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
	for k, tag := range conf.Tags {
		myNode.Tags[k] = tag
	}
	os.MkdirAll(conf.DataDir, common.DIR_PERM)

	s := &Server{
		conf:       conf,
		startTime:  time.Now(),
		maxScanJob: conf.MaxScanJob,
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
		BroadcastAddr:     conf.BroadcastAddr,
		HttpAPIPort:       conf.HttpAPIPort,
		LocalRaftAddr:     conf.LocalRaftAddr,
		DataRootDir:       conf.DataDir,
		TickMs:            conf.TickMs,
		ElectionTick:      conf.ElectionTick,
		LearnerRole:       conf.LearnerRole,
		RemoteSyncCluster: conf.RemoteSyncCluster,
		StateMachineType:  conf.StateMachineType,
		RocksDBOpts:       conf.RocksDBOpts,
	}
	if mconf.RocksDBOpts.UseSharedCache || mconf.RocksDBOpts.AdjustThreadPool || mconf.RocksDBOpts.UseSharedRateLimiter {
		sc := rockredis.NewSharedRockConfig(conf.RocksDBOpts)
		mconf.RocksDBSharedConfig = sc
	}
	s.nsMgr = node.NewNamespaceMgr(s.raftTransport, mconf)
	myNode.RegID = mconf.NodeID

	if conf.EtcdClusterAddresses != "" {
		r, err := cluster.NewDNEtcdRegister(conf.EtcdClusterAddresses)
		if err != nil {
			sLog.Fatalf("failed to init register for coordinator: %v", err)
		}
		s.dataCoord = datanode_coord.NewDataCoordinator(conf.ClusterID, myNode, s.nsMgr)
		if err := s.dataCoord.SetRegister(r); err != nil {
			sLog.Fatalf("failed to init register for coordinator: %v", err)
		}
		s.raftTransport.ID = types.ID(s.dataCoord.GetMyRegID())
		s.nsMgr.SetIClusterInfo(s.dataCoord)
	} else {
		s.raftTransport.ID = types.ID(myNode.RegID)
	}

	return s
}

func (s *Server) Stop() {
	sLog.Infof("server begin stopping")
	if s.dataCoord != nil {
		s.dataCoord.Stop()
	} else {
		s.nsMgr.Stop()
	}
	select {
	case <-s.stopC:
		sLog.Infof("server already stopped")
		return
	default:
	}
	close(s.stopC)
	s.raftTransport.Stop()
	s.wg.Wait()
	sLog.Infof("server stopped")
}

func (s *Server) GetCoord() *datanode_coord.DataCoordinator {
	return s.dataCoord
}

func (s *Server) GetNamespace(ns string, pk []byte) (*node.NamespaceNode, error) {
	return s.nsMgr.GetNamespaceNodeWithPrimaryKey(ns, pk)
}
func (s *Server) GetNamespaceFromFullName(ns string) *node.NamespaceNode {
	return s.nsMgr.GetNamespaceNode(ns)
}

func (s *Server) GetLogSyncStatsInSyncLearner() ([]common.LogSyncStats, []common.LogSyncStats) {
	return s.nsMgr.GetLogSyncStatsInSyncer()
}

func (s *Server) GetLogSyncStats(leaderOnly bool, srcClusterName string) []common.LogSyncStats {
	return s.nsMgr.GetLogSyncStats(leaderOnly, srcClusterName)
}

func (s *Server) GetTableStats(leaderOnly bool, table string) map[string]common.TableStats {
	var ss common.ServerStats
	ss.NSStats = s.nsMgr.GetStats(leaderOnly, table)
	allTbs := make(map[string]common.TableStats)
	for _, s := range ss.NSStats {
		ns, _ := common.GetNamespaceAndPartition(s.Name)
		var tbs common.TableStats
		tbs.Name = table
		if t, ok := allTbs[ns]; ok {
			tbs = t
		}
		for _, ts := range s.TStats {
			if ts.Name != table {
				continue
			}
			tbs.KeyNum += ts.KeyNum
			tbs.DiskBytesUsage += ts.DiskBytesUsage
			tbs.ApproximateKeyNum += ts.ApproximateKeyNum
		}
		if tbs.KeyNum > 0 || tbs.DiskBytesUsage > 0 || tbs.ApproximateKeyNum > 0 {
			allTbs[ns] = tbs
		}
	}
	return allTbs
}

func (s *Server) GetStats(leaderOnly bool) common.ServerStats {
	var ss common.ServerStats
	ss.NSStats = s.nsMgr.GetStats(leaderOnly, "")
	ss.ScanStats = s.scanStats.Copy()
	return ss
}

func (s *Server) GetDBStats(leaderOnly bool) map[string]string {
	return s.nsMgr.GetDBStats(leaderOnly)
}

func (s *Server) OptimizeDB(ns string, table string) {
	s.nsMgr.OptimizeDB(ns, table)
}

func (s *Server) DeleteRange(ns string, dtr node.DeleteTableRange) error {
	return s.nsMgr.DeleteRange(ns, dtr)
}

func (s *Server) InitKVNamespace(id uint64, conf *node.NamespaceConfig, join bool) (*node.NamespaceNode, error) {
	return s.nsMgr.InitNamespaceNode(conf, id, join)
}

func (s *Server) RestartAsStandalone(fullNamespace string) error {
	if s.dataCoord != nil {
		return s.dataCoord.RestartAsStandalone(fullNamespace)
	}
	return nil
}

func (s *Server) Start() {
	s.raftTransport.Start()
	s.stopC = make(chan struct{})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveRaft(s.stopC)
	}()
	s.wg.Add(1)
	// redis api enable first, because there are many partitions, some partitions may recover first
	// and become leader. In this way we need redis api enabled to allow r/w these partitions.
	go func() {
		defer s.wg.Done()
		s.serveRedisAPI(s.conf.RedisAPIPort, s.stopC)
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveGRPCAPI(s.conf.GrpcAPIPort, s.stopC)
	}()

	if s.dataCoord != nil {
		err := s.dataCoord.Start()
		if err != nil {
			sLog.Fatalf("data coordinator start failed: %v", err)
		}
	} else {
		s.nsMgr.Start()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveHttpAPI(s.conf.HttpAPIPort, s.stopC)
	}()
}

func (s *Server) GetHandler(cmdName string, cmd redcon.Command) (bool, common.CommandFunc, redcon.Command, error) {
	if len(cmd.Args) < 2 {
		return false, nil, cmd, common.ErrInvalidArgs
	}
	rawKey := cmd.Args[1]

	namespace, pk, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		sLog.Infof("failed to get the namespace of the redis command:%v", string(rawKey))
		return false, nil, cmd, err
	}
	// we need decide the partition id from the primary key
	// if the command need cross multi partitions, we need handle separate
	n, err := s.nsMgr.GetNamespaceNodeWithPrimaryKey(namespace, pk)
	if err != nil {
		return false, nil, cmd, err
	}
	// TODO: for multi primary keys such as mset, mget, we need make sure they are all in the same partition
	h, isWrite, ok := n.Node.GetHandler(cmdName)
	if !ok {
		return isWrite, nil, cmd, common.ErrInvalidCommand
	}
	if sLog.Level() >= common.LOG_DETAIL {
		sLog.Debugf("hash to namespace :%v, %v, %v, for pk:%v", isWrite, n.FullName(), n.Node.IsLead(), string(pk))
	}
	if !isWrite && !n.Node.IsLead() && (atomic.LoadInt32(&allowStaleRead) == 0) {
		// read only to leader to avoid stale read
		// TODO: also read command can request the raft read index if not leader
		return isWrite, nil, cmd, node.ErrNamespaceNotLeader
	}
	return isWrite, h, cmd, nil
}

func (s *Server) serveRaft(stopCh <-chan struct{}) {
	url, err := url.Parse(s.conf.LocalRaftAddr)
	if err != nil {
		sLog.Fatalf("failed parsing raft url: %v", err)
	}
	ln, err := common.NewStoppableListener(url.Host, stopCh)
	if err != nil {
		sLog.Fatalf("failed to listen rafthttp : %v", err)
	}
	err = (&http.Server{Handler: s.raftTransport.Handler()}).Serve(ln)
	select {
	case <-stopCh:
	default:
		sLog.Errorf("failed to serve rafthttp : %v", err)
	}
	sLog.Infof("raft http transport exit")
}

// implement the Raft interface for transport
func (s *Server) Process(ctx context.Context, m raftpb.Message) error {
	if m.Type == raftpb.MsgVoteResp {
		sLog.Infof("got message from raft transport %v ", m.String())
	}
	kv := s.nsMgr.GetNamespaceNodeFromGID(m.ToGroup.GroupId)
	if kv == nil {
		sLog.Errorf("from %v, to %v(%v), kv namespace not found while processing %v, %v, %v ",
			m.From, m.To, m.ToGroup.String(), m.Type, m.Index, m.Term)
		return node.ErrNamespacePartitionNotFound
	}
	if !kv.IsReady() {
		return errRaftGroupNotReady
	}
	return kv.Node.Process(ctx, m)
}

func (s *Server) IsPeerRemoved(peerID uint64) bool { return false }

func (s *Server) ReportUnreachable(id uint64, group raftpb.Group) {
	//sLog.Infof("report node %v in group %v unreachable", id, group)
	kv := s.nsMgr.GetNamespaceNodeFromGID(group.GroupId)
	if kv == nil {
		sLog.Errorf("kv namespace not found %v ", group.GroupId)
		return
	}
	if !kv.IsReady() {
		return
	}
	kv.Node.ReportUnreachable(id, group)
}

func (s *Server) ReportSnapshot(id uint64, gp raftpb.Group, status raft.SnapshotStatus) {
	sLog.Infof("node %v in group %v snapshot status: %v", id, gp, status)
	kv := s.nsMgr.GetNamespaceNodeFromGID(gp.GroupId)
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
func (s *Server) SaveDBFrom(r io.Reader, msg raftpb.Message) (int64, error) {
	kv := s.nsMgr.GetNamespaceNodeFromGID(msg.ToGroup.GroupId)
	if kv == nil {
		return 0, node.ErrNamespacePartitionNotFound
	}
	if !kv.IsReady() {
		return 0, errRaftGroupNotReady
	}

	return kv.Node.SaveDBFrom(r, msg)
}

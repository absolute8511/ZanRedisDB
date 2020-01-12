package server

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"github.com/youzan/ZanRedisDB/engine"

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

type writeQ struct {
	q      *entryQueue
	stopC  chan struct{}
	readyC chan struct{}
}

func newWriteQ(len uint64) *writeQ {
	return &writeQ{
		q:      newEntryQueue(len, 0),
		stopC:  make(chan struct{}),
		readyC: make(chan struct{}, 1),
	}
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
	writeQs       []*writeQ
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
	if conf.DefaultSnapCount > 0 {
		common.DefaultSnapCount = conf.DefaultSnapCount
	}
	if conf.DefaultSnapCatchup > 0 {
		common.DefaultSnapCatchup = conf.DefaultSnapCatchup
	}
	if conf.ProposalQueueNum <= 0 {
		conf.ProposalQueueNum = runtime.NumCPU()
	}
	if conf.ProposalQueueLen <= 0 {
		conf.ProposalQueueLen = 1024 * 4
	}
	writeQs := make([]*writeQ, conf.ProposalQueueNum)
	for i := 0; i < conf.ProposalQueueNum; i++ {
		writeQs[i] = newWriteQ(uint64(conf.ProposalQueueLen))
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
		writeQs:    writeQs,
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
		KeepBackup:        conf.KeepBackup,
		KeepWAL:           conf.KeepWAL,
		UseRocksWAL:       conf.UseRocksWAL,
		SharedRocksWAL:    conf.SharedRocksWAL,
		LearnerRole:       conf.LearnerRole,
		RemoteSyncCluster: conf.RemoteSyncCluster,
		StateMachineType:  conf.StateMachineType,
		RocksDBOpts:       conf.RocksDBOpts,
		WALRocksDBOpts:    conf.WALRocksDBOpts,
	}
	if mconf.RocksDBOpts.UseSharedCache || mconf.RocksDBOpts.AdjustThreadPool || mconf.RocksDBOpts.UseSharedRateLimiter {
		sc := engine.NewSharedRockConfig(conf.RocksDBOpts)
		mconf.RocksDBSharedConfig = sc
	}

	if mconf.UseRocksWAL {
		if mconf.WALRocksDBOpts.UseSharedCache || mconf.WALRocksDBOpts.AdjustThreadPool || mconf.WALRocksDBOpts.UseSharedRateLimiter {
			sc := engine.NewSharedRockConfig(conf.WALRocksDBOpts)
			mconf.WALRocksDBSharedConfig = sc
		}
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
	s.nsMgr.BackupDB("")
	// wait backup done
	time.Sleep(time.Second * 3)
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
	for _, wq := range s.writeQs {
		close(wq.stopC)
	}
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

func (s *Server) GetWALDBStats(leaderOnly bool) map[string]map[string]interface{} {
	return s.nsMgr.GetWALDBStats(leaderOnly)
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
	go func() {
		defer s.wg.Done()
		s.handleRedisWriteLoop()
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

func (s *Server) GetPKAndHashSum(cmdName string, cmd redcon.Command) (string, []byte, int, error) {
	if len(cmd.Args) < 2 {
		return "", nil, 0, common.ErrInvalidArgs
	}
	rawKey := cmd.Args[1]

	namespace, pk, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		return namespace, nil, 0, err
	}
	pkSum := int(murmur3.Sum32(pk))
	return namespace, pk, pkSum, nil
}

func (s *Server) GetHandleNode(ns string, pk []byte, pkSum int, cmdName string,
	cmd redcon.Command) (*node.KVNode, error) {
	if len(cmd.Args) < 2 {
		return nil, common.ErrInvalidArgs
	}
	// we need decide the partition id from the primary key
	// if the command need cross multi partitions, we need handle separate
	n, err := s.nsMgr.GetNamespaceNodeWithPrimaryKeySum(ns, pk, pkSum)
	if err != nil {
		return nil, err
	}
	if n.Node.IsStopping() {
		return nil, common.ErrStopped
	}
	return n.Node, nil
}

func (s *Server) GetHandler(cmdName string,
	cmd redcon.Command, kvn *node.KVNode) (common.CommandFunc, redcon.Command, error) {
	// for multi primary keys such as mset, mget, we need make sure they are all in the same partition
	h, ok := kvn.GetHandler(cmdName)
	if !ok {
		return nil, cmd, common.ErrInvalidCommand
	}
	if !kvn.IsLead() && (atomic.LoadInt32(&allowStaleRead) == 0) {
		// read only to leader to avoid stale read
		// TODO: also read command can request the raft read index if not leader
		return nil, cmd, node.ErrNamespaceNotLeader
	}
	return h, cmd, nil
}

func (s *Server) GetWriteHandler(cmdName string,
	cmd redcon.Command, kvn *node.KVNode) (common.WriteCommandFunc, redcon.Command, error) {
	h, ok := kvn.GetWriteHandler(cmdName)
	if !ok {
		return nil, cmd, common.ErrInvalidCommand
	}
	return h, cmd, nil
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
		sLog.Debugf("got vote resp message from raft transport %v ", m.String())
	}

	if len(m.Entries) > 0 {
		// we debug the slow raft log transfer
		level := atomic.LoadInt32(&costStatsLevel)
		evnt := m.Entries[0]
		if evnt.Data != nil && level > 2 {
			var reqList node.BatchInternalRaftRequest
			err := reqList.Unmarshal(evnt.Data)
			if err == nil && reqList.Timestamp > 0 {
				n := time.Now().UnixNano()
				rt := n - reqList.Timestamp
				if rt > int64(time.Millisecond*100) {
					sLog.Warningf("receive raft request slow cost: %v, src: %v", rt, reqList.ReqNum)
					if len(m.Entries) > 1 || len(reqList.Reqs) > 1 {
						oldest := reqList.Reqs[0].Header.Timestamp
						newest := m.Entries[len(m.Entries)-1]
						reqList.Unmarshal(newest.Data)
						if len(reqList.Reqs) > 0 {
							diff := reqList.Reqs[len(reqList.Reqs)-1].Header.Timestamp - oldest
							sLog.Infof("recieve raft request slow, max time diff: %v", diff)
						}
					}
				}
			}
		}
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

func (s *Server) GetNsMgr() *node.NamespaceMgr {
	return s.nsMgr
}

func (s *Server) handleRedisWrite(pkSum int, h common.WriteCommandFunc, kvn *node.KVNode, conn redcon.Conn, cmd redcon.Command) {
	// queue and wait
	wq := s.writeQs[pkSum%len(s.writeQs)]
	e := elemT{}
	ret := make(chan interface{}, 1)
	e.f = func() {
		rsp, err := h(cmd)
		if err != nil {
			ret <- err
			return
		}
		ret <- rsp
	}
	wq.q.add(e)
	select {
	case wq.readyC <- struct{}{}:
	default:
	}
	// wait
	v := <-ret
	//futureW, ok := v.(futureRspWait)
	//if ok {
	//	frsp, err := futureW.WaitRsp()
	//	if err != nil {
	//		v = err
	//	} else {
	//		if futureW.rspHandle != nil {
	//			v = futureW.rspHandle(frsp)
	//		}
	//	}
	//}
	switch rv := v.(type) {
	case error:
		conn.WriteError(rv.Error())
	case string:
		conn.WriteString(rv)
	case int64:
		conn.WriteInt64(rv)
	case int:
		conn.WriteInt64(int64(rv))
	case nil:
		conn.WriteNull()
	case []byte:
		conn.WriteBulk(rv)
	case [][]byte:
		conn.WriteArray(len(rv))
		for _, d := range rv {
			conn.WriteBulk(d)
		}
	default:
		// Do we have any other resp arrays for write command which is not [][]byte?
		conn.WriteError("Invalid response type")
	}
}

func (s *Server) handleRedisWriteLoop() {
	var wg sync.WaitGroup
	for i := 0; i < len(s.writeQs); i++ {
		wg.Add(1)
		go func(wq *writeQ) {
			defer wg.Done()
			for {
				select {
				case <-wq.readyC:
					elems := wq.q.get(false)
					for _, e := range elems {
						e.Func()()
					}
				case <-wq.stopC:
					return
				}
			}
		}(s.writeQs[i])
	}
	wg.Wait()
}

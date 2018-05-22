package node

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/rockredis"
	"github.com/youzan/ZanRedisDB/transport/rafthttp"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

var (
	ErrNamespaceAlreadyExist      = errors.New("namespace already exist")
	ErrRaftIDMismatch             = errors.New("raft id mismatch")
	ErrRaftConfMismatch           = errors.New("raft config mismatch")
	errTimeoutLeaderTransfer      = errors.New("raft leader transfer failed")
	errStopping                   = errors.New("ERR_CLUSTER_CHANGED: the namespace is stopping")
	ErrNamespaceNotFound          = errors.New("ERR_CLUSTER_CHANGED: namespace is not found")
	ErrNamespacePartitionNotFound = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not found")
	ErrNamespaceNotLeader         = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not leader on the node")
	ErrNodeNoLeader               = errors.New("ERR_CLUSTER_CHANGED: partition of the node has no leader")
	ErrRaftGroupNotReady          = errors.New("ERR_CLUSTER_CHANGED: raft group not ready")
	ErrProposalCanceled           = errors.New("ERR_CLUSTER_CHANGED: raft proposal " + context.Canceled.Error())
	errNamespaceConfInvalid       = errors.New("namespace config is invalid")
)

var perfLevel int32

type NamespaceNode struct {
	Node  *KVNode
	conf  *NamespaceConfig
	ready int32
}

func (nn *NamespaceNode) IsReady() bool {
	return atomic.LoadInt32(&nn.ready) == 1
}

func (nn *NamespaceNode) FullName() string {
	return nn.conf.Name
}

func (nn *NamespaceNode) SwitchForLearnerLeader(isLearnerLeader bool) {
	nn.Node.switchForLearnerLeader(isLearnerLeader)
}

func (nn *NamespaceNode) SetDynamicInfo(dync NamespaceDynamicConf) {
}

func (nn *NamespaceNode) SetMagicCode(magic int64) error {
	return nil
}

func (nn *NamespaceNode) SetDataFixState(needFix bool) {
}

func (nn *NamespaceNode) GetLastLeaderChangedTime() int64 {
	return nn.Node.GetLastLeaderChangedTime()
}

func (nn *NamespaceNode) GetRaftID() uint64 {
	return nn.Node.rn.config.ID
}

func (nn *NamespaceNode) CheckRaftConf(raftID uint64, conf *NamespaceConfig) error {
	if nn.conf.EngType != conf.EngType ||
		nn.conf.RaftGroupConf.GroupID != conf.RaftGroupConf.GroupID {
		nodeLog.Infof("mine :%v, check raft conf:%v", nn.conf, conf)
		return ErrRaftConfMismatch
	}
	if raftID != nn.Node.rn.config.ID {
		nodeLog.Infof("mine :%v, check raft conf:%v", nn.Node.rn.config.ID, raftID)
		return ErrRaftIDMismatch
	}
	return nil
}

func (nn *NamespaceNode) Close() {
	nn.Node.Stop()
	atomic.StoreInt32(&nn.ready, 0)
	nodeLog.Infof("namespace stopped: %v", nn.conf.Name)
}

func (nn *NamespaceNode) Destroy() error {
	return nn.Node.destroy()
}

func (nn *NamespaceNode) IsDataNeedFix() bool {
	return false
}

// full ready node means: all local commit log replay done and we are aware of leader and
// maybe we have done all the newest commit log in state machine.
func (nn *NamespaceNode) IsNsNodeFullReady(checkCommitIndex bool) bool {
	if !nn.IsReady() {
		return false
	}
	if !nn.Node.rn.IsReplayFinished() {
		return false
	}
	return nn.Node.IsRaftSynced(checkCommitIndex)
}

func (nn *NamespaceNode) GetLearners() []*common.MemberInfo {
	return nn.Node.GetLearners()
}

func (nn *NamespaceNode) GetMembers() []*common.MemberInfo {
	return nn.Node.GetMembers()
}

func (nn *NamespaceNode) Start(forceStandaloneCluster bool) error {
	if err := nn.Node.Start(forceStandaloneCluster); err != nil {
		return err
	}
	atomic.StoreInt32(&nn.ready, 1)
	return nil
}

func (nn *NamespaceNode) TransferMyLeader(to uint64, toRaftID uint64) error {
	waitTimeout := time.Duration(nn.Node.machineConfig.ElectionTick) * time.Duration(nn.Node.machineConfig.TickMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	oldLeader := nn.Node.rn.Lead()
	nn.Node.rn.node.TransferLeadership(ctx, oldLeader, toRaftID)
	for nn.Node.rn.Lead() != toRaftID {
		select {
		case <-ctx.Done():
			return errTimeoutLeaderTransfer
		case <-time.After(200 * time.Millisecond):
		}
	}
	nodeLog.Infof("finished transfer from %v to %v:%v", oldLeader, to, toRaftID)
	return nil
}

type NamespaceMeta struct {
	PartitionNum int
}

type NamespaceMgr struct {
	mutex         sync.RWMutex
	kvNodes       map[string]*NamespaceNode
	nsMetas       map[string]NamespaceMeta
	groups        map[uint64]string
	machineConf   *MachineConfig
	raftTransport *rafthttp.Transport
	stopping      int32
	stopC         chan struct{}
	wg            sync.WaitGroup
	clusterInfo   common.IClusterInfo
	newLeaderChan chan string
}

func NewNamespaceMgr(transport *rafthttp.Transport, conf *MachineConfig) *NamespaceMgr {
	ns := &NamespaceMgr{
		kvNodes:       make(map[string]*NamespaceNode),
		groups:        make(map[uint64]string),
		nsMetas:       make(map[string]NamespaceMeta),
		raftTransport: transport,
		machineConf:   conf,
		newLeaderChan: make(chan string, 2048),
	}
	regID, err := ns.LoadMachineRegID()
	if err != nil {
		nodeLog.Infof("load my register node id failed: %v", err)
	} else if regID > 0 {
		ns.machineConf.NodeID = regID
	}
	return ns
}

func (nsm *NamespaceMgr) SetIClusterInfo(clusterInfo common.IClusterInfo) {
	nsm.clusterInfo = clusterInfo
}

func (nsm *NamespaceMgr) LoadMachineRegID() (uint64, error) {
	d, err := ioutil.ReadFile(
		path.Join(nsm.machineConf.DataRootDir, "myid"),
	)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseInt(strings.Trim(string(d), "\r\n"), 10, 64)
	return uint64(v), err
}

func (nsm *NamespaceMgr) SaveMachineRegID(regID uint64) error {
	nsm.machineConf.NodeID = regID
	return ioutil.WriteFile(
		path.Join(nsm.machineConf.DataRootDir, "myid"),
		[]byte(strconv.FormatInt(int64(regID), 10)),
		common.FILE_PERM)
}

func (nsm *NamespaceMgr) Start() {
	nsm.stopC = make(chan struct{})
	atomic.StoreInt32(&nsm.stopping, 0)
	nsm.mutex.Lock()
	for _, kv := range nsm.kvNodes {
		kv.Start(false)
	}
	nsm.mutex.Unlock()
	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.clearUnusedRaftPeer()
	}()

	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.checkNamespaceRaftLeader()
	}()

	nsm.wg.Add(1)
	go func() {
		defer nsm.wg.Done()
		nsm.processRaftTick()
	}()
}

func (nsm *NamespaceMgr) Stop() {
	if !atomic.CompareAndSwapInt32(&nsm.stopping, 0, 1) {
		return
	}
	close(nsm.stopC)
	tmp := nsm.GetNamespaces()
	for _, n := range tmp {
		n.Close()
	}
	nsm.wg.Wait()
	nodeLog.Infof("namespace manager stopped")
	if nsm.machineConf.RocksDBSharedConfig != nil {
		nsm.machineConf.RocksDBSharedConfig.Destroy()
	}
}

func (nsm *NamespaceMgr) IsAllRecoveryDone() bool {
	done := true
	nsm.mutex.RLock()
	for _, n := range nsm.kvNodes {
		if !n.IsNsNodeFullReady(true) {
			done = false
			break
		}
	}
	nsm.mutex.RUnlock()
	return done
}

func (nsm *NamespaceMgr) GetNamespaces() map[string]*NamespaceNode {
	tmp := make(map[string]*NamespaceNode)
	nsm.mutex.RLock()
	for k, n := range nsm.kvNodes {
		tmp[k] = n
	}
	nsm.mutex.RUnlock()
	return tmp
}

func (nsm *NamespaceMgr) InitNamespaceNode(conf *NamespaceConfig, raftID uint64, join bool) (*NamespaceNode, error) {
	if atomic.LoadInt32(&nsm.stopping) == 1 {
		return nil, errStopping
	}

	expPolicy, err := common.StringToExpirationPolicy(conf.ExpirationPolicy)
	if err != nil {
		nodeLog.Infof("namespace %v invalid expire policy : %v", conf.Name, conf.ExpirationPolicy)
		return nil, err
	}

	kvOpts := &KVOptions{
		DataDir:          path.Join(nsm.machineConf.DataRootDir, conf.Name),
		EngType:          conf.EngType,
		RockOpts:         nsm.machineConf.RocksDBOpts,
		ExpirationPolicy: expPolicy,
		SharedConfig:     nsm.machineConf.RocksDBSharedConfig,
	}
	rockredis.FillDefaultOptions(&kvOpts.RockOpts)

	if conf.PartitionNum <= 0 {
		return nil, errNamespaceConfInvalid
	}
	if conf.Replicator <= 0 {
		return nil, errNamespaceConfInvalid
	}
	clusterNodes := make(map[uint64]ReplicaInfo)
	for _, v := range conf.RaftGroupConf.SeedNodes {
		clusterNodes[v.ReplicaID] = v
	}
	_, ok := clusterNodes[uint64(raftID)]
	if !ok {
		join = true
	}

	nsm.mutex.Lock()
	defer nsm.mutex.Unlock()
	if n, ok := nsm.kvNodes[conf.Name]; ok {
		return n, ErrNamespaceAlreadyExist
	}

	d, _ := json.MarshalIndent(&conf, "", " ")
	nodeLog.Infof("namespace load config: %v", string(d))
	d, _ = json.MarshalIndent(&kvOpts, "", " ")
	nodeLog.Infof("namespace kv config: %v", string(d))
	nodeLog.Infof("local namespace node %v start with raft cluster: %v", raftID, clusterNodes)
	raftConf := &RaftConfig{
		GroupID:        conf.RaftGroupConf.GroupID,
		GroupName:      conf.Name,
		ID:             uint64(raftID),
		RaftAddr:       nsm.machineConf.LocalRaftAddr,
		DataDir:        kvOpts.DataDir,
		RaftPeers:      clusterNodes,
		SnapCount:      conf.SnapCount,
		SnapCatchup:    conf.SnapCatchup,
		Replicator:     conf.Replicator,
		OptimizedFsync: conf.OptimizedFsync,
		KeepWAL:        nsm.machineConf.KeepWAL,
	}
	kv, err := NewKVNode(kvOpts, nsm.machineConf, raftConf, nsm.raftTransport,
		join, nsm.onNamespaceDeleted(raftConf.GroupID, conf.Name),
		nsm.clusterInfo, nsm.newLeaderChan)
	if err != nil {
		return nil, err
	}
	if _, ok := nsm.nsMetas[conf.BaseName]; !ok {
		nsm.nsMetas[conf.BaseName] = NamespaceMeta{
			PartitionNum: conf.PartitionNum,
		}
	}

	n := &NamespaceNode{
		Node: kv,
		conf: conf,
	}

	nsm.kvNodes[conf.Name] = n
	nsm.groups[raftConf.GroupID] = conf.Name
	return n, nil
}

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

func (nsm *NamespaceMgr) GetNamespaceNodeWithPrimaryKey(nsBaseName string, pk []byte) (*NamespaceNode, error) {
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	v, ok := nsm.nsMetas[nsBaseName]
	if !ok {
		nodeLog.Infof("namespace %v meta not found", nsBaseName)
		return nil, ErrNamespaceNotFound
	}
	pid := GetHashedPartitionID(pk, v.PartitionNum)
	fullName := common.GetNsDesp(nsBaseName, pid)
	n, ok := nsm.kvNodes[fullName]
	if !ok {
		nodeLog.Debugf("namespace %v partition %v not found for pk: %v", nsBaseName, pid, string(pk))
		return nil, ErrNamespacePartitionNotFound
	}
	if !n.IsReady() {
		return nil, ErrRaftGroupNotReady
	}
	return n, nil
}

func (nsm *NamespaceMgr) GetNamespaceNodes(nsBaseName string, leaderOnly bool) (map[string]*NamespaceNode, error) {
	nsNodes := make(map[string]*NamespaceNode)

	tmp := nsm.GetNamespaces()
	for k, v := range tmp {
		ns, _ := common.GetNamespaceAndPartition(k)
		if ns == nsBaseName && v.IsReady() {
			if leaderOnly && !v.Node.IsLead() {
				continue
			}
			nsNodes[k] = v
		}
	}

	if len(nsNodes) <= 0 {
		return nil, ErrNamespaceNotFound
	}

	return nsNodes, nil
}

func (nsm *NamespaceMgr) GetNamespaceNode(ns string) *NamespaceNode {
	nsm.mutex.RLock()
	v, _ := nsm.kvNodes[ns]
	nsm.mutex.RUnlock()
	if v != nil {
		if !v.IsReady() {
			return nil
		}
	}
	return v
}

func (nsm *NamespaceMgr) GetNamespaceNodeFromGID(gid uint64) *NamespaceNode {
	nsm.mutex.RLock()
	defer nsm.mutex.RUnlock()
	gn, ok := nsm.groups[gid]
	if !ok {
		nodeLog.Debugf("group name not found %v ", gid)
		return nil
	}
	kv, ok := nsm.kvNodes[gn]
	if !ok {
		nodeLog.Infof("kv namespace not found %v ", gn)
		return nil
	}

	if !kv.IsReady() {
		return nil
	}
	return kv
}

func (nsm *NamespaceMgr) GetDBStats(leaderOnly bool) map[string]string {
	nsm.mutex.RLock()
	nsStats := make(map[string]string, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		dbStats := n.Node.GetDBInternalStats()
		nsStats[k] = dbStats
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) GetLogSyncStatsInSyncer() ([]common.LogSyncStats, []common.LogSyncStats) {
	nsm.mutex.RLock()
	nsRecvStats := make([]common.LogSyncStats, 0, len(nsm.kvNodes))
	nsSyncStats := make([]common.LogSyncStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		recvStats, syncStats := n.Node.GetLogSyncStatsInSyncLearner()
		if recvStats == nil || syncStats == nil {
			continue
		}
		recvStats.Name = k
		syncStats.Name = k
		nsRecvStats = append(nsRecvStats, *recvStats)
		nsSyncStats = append(nsSyncStats, *syncStats)
	}
	nsm.mutex.RUnlock()
	return nsRecvStats, nsSyncStats
}

func (nsm *NamespaceMgr) GetLogSyncStats(leaderOnly bool, srcClusterName string) []common.LogSyncStats {
	if srcClusterName == "" {
		return nil
	}
	nsm.mutex.RLock()
	nsStats := make([]common.LogSyncStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		term, index, ts := n.Node.GetRemoteClusterSyncedRaft(srcClusterName)
		if term == 0 && index == 0 {
			continue
		}
		var s common.LogSyncStats
		s.Name = k
		s.IsLeader = n.Node.IsLead()
		s.Term = term
		s.Index = index
		s.Timestamp = ts
		nsStats = append(nsStats, s)
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) GetStats(leaderOnly bool) []common.NamespaceStats {
	nsm.mutex.RLock()
	nsStats := make([]common.NamespaceStats, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		ns := n.Node.GetStats()
		ns.Name = k
		ns.EngType = n.conf.EngType
		ns.IsLeader = n.Node.IsLead()
		nsStats = append(nsStats, ns)
	}
	nsm.mutex.RUnlock()
	return nsStats
}

func (nsm *NamespaceMgr) OptimizeDB(ns string, table string) {
	nsm.mutex.RLock()
	nodeList := make([]*NamespaceNode, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		baseName, _ := common.GetNamespaceAndPartition(k)
		if ns != "" && ns != baseName {
			continue
		}
		nodeList = append(nodeList, n)
	}
	nsm.mutex.RUnlock()
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.OptimizeDB(table)
		}
	}
}

func (nsm *NamespaceMgr) DeleteRange(ns string, dtr DeleteTableRange) error {
	nsm.mutex.RLock()
	nodeList := make([]*NamespaceNode, 0, len(nsm.kvNodes))
	for k, n := range nsm.kvNodes {
		baseName, _ := common.GetNamespaceAndPartition(k)
		if ns != baseName {
			continue
		}
		nodeList = append(nodeList, n)
	}
	nsm.mutex.RUnlock()
	for _, n := range nodeList {
		if atomic.LoadInt32(&nsm.stopping) == 1 {
			return common.ErrStopped
		}
		if n.IsReady() {
			err := n.Node.DeleteRange(dtr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (nsm *NamespaceMgr) onNamespaceDeleted(gid uint64, ns string) func() {
	return func() {
		nsm.mutex.Lock()
		_, ok := nsm.kvNodes[ns]
		if ok {
			nodeLog.Infof("namespace deleted: %v-%v", ns, gid)
			nsm.kvNodes[ns] = nil
			delete(nsm.kvNodes, ns)
			delete(nsm.groups, gid)
		}
		nsm.mutex.Unlock()
	}
}

func (nsm *NamespaceMgr) processRaftTick() {
	ticker := time.NewTicker(time.Duration(nsm.machineConf.TickMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// send tick for all raft group
			nsm.mutex.RLock()
			nodes := make([]*KVNode, 0, len(nsm.kvNodes))
			for _, v := range nsm.kvNodes {
				if v.IsReady() {
					nodes = append(nodes, v.Node)
				}
			}
			nsm.mutex.RUnlock()
			for _, n := range nodes {
				n.Tick()
			}

		case <-nsm.stopC:
			return
		}
	}
}

// TODO:
func (nsm *NamespaceMgr) SetNamespaceMagicCode(node *NamespaceNode, magic int64) error {
	return nil
}

func (nsm *NamespaceMgr) CheckMagicCode(ns string, magic int64, fix bool) error {
	return nil
}

func (nsm *NamespaceMgr) checkNamespaceRaftLeader() {
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()
	leaderNodes := make([]*NamespaceNode, 0)
	// report to leader may failed, so we need retry and check period
	doCheck := func() {
		leaderNodes = leaderNodes[:0]
		nsm.mutex.RLock()
		for _, v := range nsm.kvNodes {
			if v.IsReady() {
				if v.Node.IsLead() {
					leaderNodes = append(leaderNodes, v)
				}
			}
		}
		nsm.mutex.RUnlock()
		for _, v := range leaderNodes {
			v.Node.ReportMeLeaderToCluster()
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case ns := <-nsm.newLeaderChan:
			nsm.mutex.RLock()
			v, ok := nsm.kvNodes[ns]
			nsm.mutex.RUnlock()
			if !ok {
				nodeLog.Infof("leader changed namespace not found: %v", ns)
			} else if v.IsReady() {
				v.Node.OnRaftLeaderChanged()
			}
		case <-nsm.stopC:
			return
		}
	}
}

func (nsm *NamespaceMgr) clearUnusedRaftPeer() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	// while close or remove raft node, we need check if any remote transport peer
	// should be closed.
	doCheck := func() {
		nsm.mutex.RLock()
		defer nsm.mutex.RUnlock()
		peers := nsm.raftTransport.GetAllPeers()
		currentNodeIDs := make(map[uint64]bool)
		for _, v := range nsm.kvNodes {
			mems := v.Node.GetMembers()
			for _, m := range mems {
				currentNodeIDs[m.NodeID] = true
			}
			// handle learners here
			mems = v.Node.GetLearners()
			for _, m := range mems {
				currentNodeIDs[m.NodeID] = true
			}
		}
		for _, p := range peers {
			if _, ok := currentNodeIDs[uint64(p)]; !ok {
				nodeLog.Infof("remove peer %v from transport since no any raft is related", p)
				nsm.raftTransport.RemovePeer(p)
			}
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case <-nsm.stopC:
			return
		}
	}
}

func SetPerfLevel(level int) {
	atomic.StoreInt32(&perfLevel, int32(level))
	rockredis.SetPerfLevel(level)
}

func IsPerfEnabled() bool {
	lv := GetPerfLevel()
	return rockredis.IsPerfEnabledLevel(lv)
}

func GetPerfLevel() int {
	return int(atomic.LoadInt32(&perfLevel))
}

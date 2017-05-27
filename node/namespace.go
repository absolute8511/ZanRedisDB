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

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

var (
	ErrNamespaceAlreadyExist      = errors.New("namespace already exist")
	ErrRaftIDMismatch             = errors.New("raft id mismatch")
	ErrRaftConfMismatch           = errors.New("raft config mismatch")
	errTimeoutLeaderTransfer      = errors.New("raft leader transfer failed")
	errStopping                   = errors.New("the namespace is stopping")
	ErrNamespaceNotFound          = errors.New("ERR_CLUSTER_CHANGED: namespace is not found")
	ErrNamespacePartitionNotFound = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not found")
	ErrNamespaceNotLeader         = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace is not leader on the node")
	ErrNamespaceNoLeader          = errors.New("ERR_CLUSTER_CHANGED: partition of the namespace has no leader")
	ErrRaftGroupNotReady          = errors.New("raft group not ready")
	errNamespaceConfInvalid       = errors.New("namespace config is invalid")
)

type NamespaceNode struct {
	Node  *KVNode
	conf  *NamespaceConfig
	ready int32
}

func (self *NamespaceNode) IsReady() bool {
	return atomic.LoadInt32(&self.ready) == 1
}

func (self *NamespaceNode) FullName() string {
	return self.conf.Name
}

func (self *NamespaceNode) SetDynamicInfo(dync NamespaceDynamicConf) {
}

func (self *NamespaceNode) SetMagicCode(magic int64) error {
	return nil
}

func (self *NamespaceNode) SetDataFixState(needFix bool) {
}

func (self *NamespaceNode) GetLastLeaderChangedTime() int64 {
	return self.Node.GetLastLeaderChangedTime()
}

func (self *NamespaceNode) GetRaftID() uint64 {
	return self.Node.rn.config.ID
}

func (self *NamespaceNode) CheckRaftConf(raftID uint64, conf *NamespaceConfig) error {
	if self.conf.EngType != conf.EngType ||
		self.conf.RaftGroupConf.GroupID != conf.RaftGroupConf.GroupID {
		nodeLog.Infof("mine :%v, check raft conf:%v", self.conf, conf)
		return ErrRaftConfMismatch
	}
	if raftID != self.Node.rn.config.ID {
		nodeLog.Infof("mine :%v, check raft conf:%v", self.Node.rn.config.ID, raftID)
		return ErrRaftIDMismatch
	}
	return nil
}

func (self *NamespaceNode) StopRaft() {
	self.Node.StopRaft()
}

func (self *NamespaceNode) Close() {
	self.Node.Stop()
	nodeLog.Infof("namespace stopped: %v", self.conf.Name)
}

func (self *NamespaceNode) Destroy() error {
	return self.Node.destroy()
}

func (self *NamespaceNode) IsDataNeedFix() bool {
	return false
}

func (self *NamespaceNode) IsRaftSynced(checkCommitIndex bool) bool {
	return self.Node.IsRaftSynced(checkCommitIndex)
}

func (self *NamespaceNode) GetMembers() []*common.MemberInfo {
	return self.Node.GetMembers()
}

func (self *NamespaceNode) Start(forceStandaloneCluster bool) error {
	if err := self.Node.Start(forceStandaloneCluster); err != nil {
		return err
	}
	atomic.StoreInt32(&self.ready, 1)
	return nil
}

func (self *NamespaceNode) TransferMyLeader(to uint64, toRaftID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	oldLeader := self.Node.rn.Lead()
	self.Node.rn.node.TransferLeadership(ctx, oldLeader, toRaftID)
	for self.Node.rn.Lead() != toRaftID {
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
		newLeaderChan: make(chan string, 16),
		stopC:         make(chan struct{}),
	}
	regID, err := ns.LoadMachineRegID()
	if err != nil {
		nodeLog.Infof("load my register node id failed: %v", err)
	} else if regID > 0 {
		ns.machineConf.NodeID = regID
	}
	return ns
}

func (self *NamespaceMgr) SetClusterInfoInterface(clusterInfo common.IClusterInfo) {
	self.clusterInfo = clusterInfo
}

func (self *NamespaceMgr) LoadMachineRegID() (uint64, error) {
	d, err := ioutil.ReadFile(
		path.Join(self.machineConf.DataRootDir, "myid"),
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

func (self *NamespaceMgr) SaveMachineRegID(regID uint64) error {
	self.machineConf.NodeID = regID
	return ioutil.WriteFile(
		path.Join(self.machineConf.DataRootDir, "myid"),
		[]byte(strconv.FormatInt(int64(regID), 10)),
		common.FILE_PERM)
}

func (self *NamespaceMgr) Start() {
	self.mutex.Lock()
	for _, kv := range self.kvNodes {
		kv.Start(false)
	}
	self.mutex.Unlock()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.clearUnusedRaftPeer()
	}()

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.checkNamespaceRaftLeader()
	}()

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.processRaftTick()
	}()
}

func (self *NamespaceMgr) Stop() {
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
		return
	}
	close(self.stopC)
	tmp := self.GetNamespaces()
	for _, n := range tmp {
		n.Close()
	}
	self.wg.Wait()
	nodeLog.Infof("namespace manager stopped")
}

func (self *NamespaceMgr) GetNamespaces() map[string]*NamespaceNode {
	tmp := make(map[string]*NamespaceNode)
	self.mutex.RLock()
	for k, n := range self.kvNodes {
		tmp[k] = n
	}
	self.mutex.RUnlock()
	return tmp
}

func (self *NamespaceMgr) InitNamespaceNode(conf *NamespaceConfig, raftID uint64, join bool) (*NamespaceNode, error) {
	if atomic.LoadInt32(&self.stopping) == 1 {
		return nil, errStopping
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if n, ok := self.kvNodes[conf.Name]; ok {
		return n, ErrNamespaceAlreadyExist
	}

	kvOpts := &KVOptions{
		DataDir:  path.Join(self.machineConf.DataRootDir, conf.Name),
		EngType:  conf.EngType,
		RockOpts: self.machineConf.RocksDBOpts,
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

	d, _ := json.MarshalIndent(&conf, "", " ")
	nodeLog.Infof("namespace load config: %v", string(d))
	nodeLog.Infof("local namespace node %v start with raft cluster: %v", raftID, clusterNodes)
	raftConf := &RaftConfig{
		GroupID:        conf.RaftGroupConf.GroupID,
		GroupName:      conf.Name,
		ID:             uint64(raftID),
		RaftAddr:       self.machineConf.LocalRaftAddr,
		DataDir:        kvOpts.DataDir,
		RaftPeers:      clusterNodes,
		SnapCount:      conf.SnapCount,
		SnapCatchup:    conf.SnapCatchup,
		Replicator:     conf.Replicator,
		OptimizedFsync: conf.OptimizedFsync,
	}
	kv, err := NewKVNode(kvOpts, self.machineConf, raftConf, self.raftTransport,
		join, self.onNamespaceDeleted(raftConf.GroupID, conf.Name),
		self.clusterInfo, self.newLeaderChan)
	if err != nil {
		return nil, err
	}
	if _, ok := self.nsMetas[conf.BaseName]; !ok {
		self.nsMetas[conf.BaseName] = NamespaceMeta{
			PartitionNum: conf.PartitionNum,
		}
	}

	n := &NamespaceNode{
		Node: kv,
		conf: conf,
	}

	self.kvNodes[conf.Name] = n
	self.groups[raftConf.GroupID] = conf.Name
	return n, nil
}

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

func (self *NamespaceMgr) GetNamespaceNodeWithPrimaryKey(nsBaseName string, pk []byte) (*NamespaceNode, error) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	v, ok := self.nsMetas[nsBaseName]
	if !ok {
		nodeLog.Infof("namespace %v meta not found", nsBaseName)
		return nil, ErrNamespaceNotFound
	}
	pid := GetHashedPartitionID(pk, v.PartitionNum)
	fullName := common.GetNsDesp(nsBaseName, pid)
	n, ok := self.kvNodes[fullName]
	if !ok {
		nodeLog.Debugf("namespace %v partition %v not found for pk: %v", nsBaseName, pid, string(pk))
		return nil, ErrNamespacePartitionNotFound
	}
	if !n.IsReady() {
		return nil, ErrRaftGroupNotReady
	}
	return n, nil
}

func (self *NamespaceMgr) GetNamespaceNodes(nsBaseName string, leaderOnly bool) (map[string]*NamespaceNode, error) {
	nsNodes := make(map[string]*NamespaceNode)

	tmp := self.GetNamespaces()
	for k, v := range tmp {
		ns, _ := common.GetNamespaceAndPartition(k)
		if ns == nsBaseName && v.IsReady() {
			if leaderOnly && !v.Node.IsLead() {
				if v.Node.GetLeadMember() == nil {
					return nil, ErrNamespaceNoLeader
				}
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

func (self *NamespaceMgr) GetNamespaceNode(ns string) *NamespaceNode {
	self.mutex.RLock()
	v, _ := self.kvNodes[ns]
	self.mutex.RUnlock()
	if v != nil {
		if !v.IsReady() {
			return nil
		}
	}
	return v
}

func (self *NamespaceMgr) GetNamespaceNodeFromGID(gid uint64) *NamespaceNode {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	gn, ok := self.groups[gid]
	if !ok {
		nodeLog.Errorf("group name not found %v ", gid)
		return nil
	}
	kv, ok := self.kvNodes[gn]
	if !ok {
		nodeLog.Errorf("kv namespace not found %v ", gn)
		return nil
	}

	if !kv.IsReady() {
		return nil
	}
	return kv
}

func (self *NamespaceMgr) GetDBStats(leaderOnly bool) map[string]string {
	self.mutex.RLock()
	nsStats := make(map[string]string, len(self.kvNodes))
	for k, n := range self.kvNodes {
		if !n.IsReady() {
			continue
		}
		if leaderOnly && !n.Node.IsLead() {
			continue
		}
		dbStats := n.Node.GetDBInternalStats()
		nsStats[k] = dbStats
	}
	self.mutex.RUnlock()
	return nsStats
}

func (self *NamespaceMgr) GetStats(leaderOnly bool) []common.NamespaceStats {
	self.mutex.RLock()
	nsStats := make([]common.NamespaceStats, 0, len(self.kvNodes))
	for k, n := range self.kvNodes {
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
	self.mutex.RUnlock()
	return nsStats
}

func (self *NamespaceMgr) OptimizeDB() {
	self.mutex.RLock()
	nodeList := make([]*NamespaceNode, 0, len(self.kvNodes))
	for _, n := range self.kvNodes {
		nodeList = append(nodeList, n)
	}
	self.mutex.RUnlock()
	for _, n := range nodeList {
		if atomic.LoadInt32(&self.stopping) == 1 {
			return
		}
		if n.IsReady() {
			n.Node.OptimizeDB()
		}
	}
}

func (self *NamespaceMgr) onNamespaceDeleted(gid uint64, ns string) func() {
	return func() {
		self.mutex.Lock()
		_, ok := self.kvNodes[ns]
		if ok {
			nodeLog.Infof("namespace deleted: %v-%v", ns, gid)
			delete(self.kvNodes, ns)
			delete(self.groups, gid)
		}
		self.mutex.Unlock()
	}
}

func (self *NamespaceMgr) processRaftTick() {
	ticker := time.NewTicker(time.Duration(self.machineConf.TickMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// send tick for all raft group
			self.mutex.RLock()
			nodes := make([]*KVNode, 0, len(self.kvNodes))
			for _, v := range self.kvNodes {
				if v.IsReady() {
					nodes = append(nodes, v.Node)
				}
			}
			self.mutex.RUnlock()
			for _, n := range nodes {
				n.Tick()
			}

		case <-self.stopC:
			return
		}
	}
}

// TODO:
func (self *NamespaceMgr) SetNamespaceMagicCode(node *NamespaceNode, magic int64) error {
	return nil
}

func (self *NamespaceMgr) CheckMagicCode(ns string, magic int64, fix bool) error {
	return nil
}

func (self *NamespaceMgr) checkNamespaceRaftLeader() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	leaderNodes := make([]*NamespaceNode, 0)
	// while close or remove raft node, we need check if any remote transport peer
	// should be closed.
	doCheck := func() {
		leaderNodes = leaderNodes[:0]
		self.mutex.RLock()
		for _, v := range self.kvNodes {
			if v.IsReady() && v.Node.IsLead() {
				leaderNodes = append(leaderNodes, v)
			}
		}
		self.mutex.RUnlock()
		for _, v := range leaderNodes {
			v.Node.ReportMeRaftLeader()
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case ns := <-self.newLeaderChan:
			self.mutex.RLock()
			v, ok := self.kvNodes[ns]
			self.mutex.RUnlock()
			if !ok {
				nodeLog.Infof("leader changed namespace not found: %v", ns)
			} else if v.IsReady() {
				v.Node.ReportMeRaftLeader()
			}
		case <-self.stopC:
			return
		}
	}
}

func (self *NamespaceMgr) clearUnusedRaftPeer() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	// while close or remove raft node, we need check if any remote transport peer
	// should be closed.
	doCheck := func() {
		self.mutex.RLock()
		defer self.mutex.RUnlock()
		peers := self.raftTransport.GetAllPeers()
		currentNodeIDs := make(map[uint64]bool)
		for _, v := range self.kvNodes {
			mems := v.Node.GetMembers()
			for _, m := range mems {
				currentNodeIDs[m.NodeID] = true
			}
		}
		for _, p := range peers {
			if _, ok := currentNodeIDs[uint64(p)]; !ok {
				nodeLog.Infof("remove peer %v from transport since no any raft is related", p)
				self.raftTransport.RemovePeer(p)
			}
		}
	}

	for {
		select {
		case <-ticker.C:
			doCheck()
		case <-self.stopC:
			return
		}
	}
}

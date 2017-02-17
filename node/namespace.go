package node

import (
	"encoding/json"
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNamespaceAlreadyExist = errors.New("namespace already exist")
	ErrRaftIDMismatch        = errors.New("raft id mismatch")
	ErrRaftConfMismatch      = errors.New("raft config mismatch")
	errTimeoutLeaderTransfer = errors.New("raft leader transfer failed")
)

type NamespaceNode struct {
	Node  *KVNode
	conf  *NamespaceConfig
	ready int32
}

func (self *NamespaceNode) IsReady() bool {
	return atomic.LoadInt32(&self.ready) == 1
}

func (self *NamespaceNode) SetDynamicInfo(dync NamespaceDynamicConf) {
}

func (self *NamespaceNode) SetMagicCode(magic int64) error {
	return nil
}

func (self *NamespaceNode) SetDataFixState(needFix bool) {
}

func (self *NamespaceNode) CheckRaftConf(raftID uint64, conf *NamespaceConfig) error {
	if self.conf.EngType != conf.EngType ||
		self.conf.RaftGroupConf.GroupID != conf.RaftGroupConf.GroupID {
		return ErrRaftConfMismatch
	}
	if raftID != self.Node.rn.config.ID {
		return ErrRaftIDMismatch
	}
	return nil
}

func (self *NamespaceNode) Destroy() {
	self.Node.Destroy()
}

func (self *NamespaceNode) Close() {
	self.Node.Stop()
}

func (self *NamespaceNode) IsDataNeedFix() bool {
	return false
}

func (self *NamespaceNode) IsRaftSynced() bool {
	return self.Node.IsRaftSynced()
}

func (self *NamespaceNode) GetMembers() []*MemberInfo {
	return self.Node.GetMembers()
}

func (self *NamespaceNode) TransferMyLeader(to uint64) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	oldLeader := self.Node.rn.Lead()
	self.Node.rn.node.TransferLeadership(ctx, oldLeader, to)
	for self.Node.rn.Lead() != to {
		select {
		case <-ctx.Done():
			return errTimeoutLeaderTransfer
		case <-time.After(200 * time.Millisecond):
		}
	}
	nodeLog.Infof("finished transfer from %v to %v", oldLeader, to)
	cancel()
	return nil
}

type NamespaceMgr struct {
	mutex         sync.Mutex
	kvNodes       map[string]*NamespaceNode
	groups        map[uint64]string
	machineConf   *MachineConfig
	raftTransport *rafthttp.Transport
}

func NewNamespaceMgr(transport *rafthttp.Transport, conf *MachineConfig) *NamespaceMgr {
	ns := &NamespaceMgr{
		kvNodes:       make(map[string]*NamespaceNode),
		groups:        make(map[uint64]string),
		raftTransport: transport,
		machineConf:   conf,
	}
	regID, err := ns.LoadMachineRegID()
	if err != nil {
		nodeLog.Infof("load my register node id failed: %v", err)
	} else if regID > 0 {
		ns.machineConf.NodeID = regID
	}

	return ns
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
		kv.Node.Start()
		atomic.StoreInt32(&kv.ready, 1)
	}
	self.mutex.Unlock()
}

func (self *NamespaceMgr) Stop() {
	tmp := self.GetNamespaces()
	for k, n := range tmp {
		n.Node.Stop()
		nodeLog.Infof("kv namespace stopped: %v", k)
	}
}

func (self *NamespaceMgr) GetNamespaces() map[string]*NamespaceNode {
	tmp := make(map[string]*NamespaceNode)
	self.mutex.Lock()
	for k, n := range self.kvNodes {
		tmp[k] = n
	}
	self.mutex.Unlock()
	return tmp
}

func (self *NamespaceMgr) InitNamespaceNode(conf *NamespaceConfig, raftID uint64) (*NamespaceNode, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if n, ok := self.kvNodes[conf.Name]; ok {
		return n, ErrNamespaceAlreadyExist
	}

	kvOpts := &KVOptions{
		DataDir: path.Join(self.machineConf.DataRootDir, conf.Name),
		EngType: conf.EngType,
	}
	if conf.PartitionNum == 0 {
		conf.PartitionNum = 1
	}
	clusterNodes := make(map[uint64]ReplicaInfo)
	for _, v := range conf.RaftGroupConf.SeedNodes {
		clusterNodes[v.ReplicaID] = v
	}
	_, ok := clusterNodes[uint64(raftID)]
	join := false
	if !ok {
		join = true
	}

	d, _ := json.MarshalIndent(&conf, "", " ")
	nodeLog.Infof("namespace load config: %v", string(d))
	nodeLog.Infof("local namespace node %v start with raft cluster: %v", raftID, clusterNodes)

	raftConf := &RaftConfig{
		GroupID:     conf.RaftGroupConf.GroupID,
		GroupName:   conf.Name,
		ID:          uint64(raftID),
		RaftAddr:    self.machineConf.LocalRaftAddr,
		DataDir:     kvOpts.DataDir,
		RaftPeers:   clusterNodes,
		SnapCount:   conf.SnapCount,
		SnapCatchup: conf.SnapCatchup,
	}
	kv := NewKVNode(kvOpts, self.machineConf, raftConf, self.raftTransport,
		join, self.onNamespaceDeleted(raftConf.GroupID, conf.Name))
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

func (self *NamespaceMgr) GetNamespaceNodeWithPrimaryKey(ns string, pk []byte) *NamespaceNode {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	v, ok := self.kvNodes[ns+"-0"]
	if !ok {
		return nil
	}
	v, ok = self.kvNodes[ns+"-"+strconv.Itoa(GetHashedPartitionID(pk, v.conf.PartitionNum))]
	if !ok {
		return nil
	}
	return v
}

func (self *NamespaceMgr) GetNamespaceNode(ns string) *NamespaceNode {
	self.mutex.Lock()
	v, _ := self.kvNodes[ns]
	self.mutex.Unlock()
	return v
}

func (self *NamespaceMgr) GetNamespaceNodeFromGID(gid uint64) *NamespaceNode {
	self.mutex.Lock()
	defer self.mutex.Unlock()
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

	return kv
}

func (self *NamespaceMgr) GetStats() []common.NamespaceStats {
	self.mutex.Lock()
	nsStats := make([]common.NamespaceStats, 0, len(self.kvNodes))
	for k, n := range self.kvNodes {
		ns := n.Node.GetStats()
		ns.Name = k
		ns.EngType = n.conf.EngType
		nsStats = append(nsStats, ns)
	}
	self.mutex.Unlock()
	return nsStats
}

func (self *NamespaceMgr) OptimizeDB() {
	self.mutex.Lock()
	nodeList := make([]*NamespaceNode, 0, len(self.kvNodes))
	for _, n := range self.kvNodes {
		nodeList = append(nodeList, n)
	}
	self.mutex.Unlock()
	for _, n := range nodeList {
		n.Node.OptimizeDB()
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

func (self *NamespaceMgr) ProcessRaftTick() {
	// send tick for all raft group
	self.mutex.Lock()
	nodes := make([]*KVNode, 0, len(self.kvNodes))
	for _, v := range self.kvNodes {
		if v.IsReady() {
			nodes = append(nodes, v.Node)
		}
	}
	self.mutex.Unlock()
	for _, n := range nodes {
		n.Tick()
	}
}

func (self *NamespaceMgr) SetNamespaceMagicCode(node *NamespaceNode, magic int64) error {
	return nil
}

func (self *NamespaceMgr) CheckMagicCode(ns string, magic int64, fix bool) error {
	return nil
}

func (self *NamespaceMgr) ForceDeleteNamespaceData(ns string) error {
	nsNode := self.GetNamespaceNode(ns)
	if nsNode == nil {
		return errors.New("namespace not found: " + ns)
	}
	nsNode.Destroy()
	return nil
}

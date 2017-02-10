package node

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/transport/rafthttp"
	"path"
	"sync"
	"sync/atomic"
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

func (self *NamespaceNode) SetDataFixState(needFix bool) {
}

func (self *NamespaceNode) Close() {
	self.Node.Stop()
}

func (self *NamespaceNode) IsDataNeedFix() bool {
	return false
}

type NamespaceMgr struct {
	mutex   sync.Mutex
	kvNodes map[string]*NamespaceNode
	groups  map[uint64]string
}

func NewNamespaceMgr() *NamespaceMgr {
	ns := &NamespaceMgr{
		kvNodes: make(map[string]*NamespaceNode),
		groups:  make(map[uint64]string),
	}
	return ns
}

func (self *NamespaceMgr) Start() {
	for _, kv := range self.kvNodes {
		kv.Node.Start()
		atomic.StoreInt32(&kv.ready, 1)
	}
}

func (self *NamespaceMgr) Stop() {
	self.mutex.Lock()
	for k, n := range self.kvNodes {
		n.Node.Stop()
		nodeLog.Infof("kv namespace stopped: %v", k)
	}
	self.mutex.Unlock()
}

func (self *NamespaceMgr) InitNamespaceNode(dataDir string, mConf *MachineConfig, conf *NamespaceConfig,
	transport *rafthttp.Transport, raftID uint64, join bool) error {
	kvOpts := &KVOptions{
		DataDir: path.Join(dataDir, conf.Name),
		EngType: conf.EngType,
	}
	clusterNodes := make(map[uint64]ReplicaInfo)
	for _, v := range conf.RaftGroupConf.SeedNodes {
		clusterNodes[v.ReplicaID] = v
	}

	raftConf := &RaftConfig{
		GroupID:     conf.RaftGroupConf.GroupID,
		GroupName:   conf.Name,
		ID:          uint64(raftID),
		RaftAddr:    mConf.LocalRaftAddr,
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
	kv := NewKVNode(kvOpts, mConf, raftConf, transport,
		join, self.onNamespaceDeleted(raftConf.GroupID, conf.Name))
	n := &NamespaceNode{
		Node: kv,
		conf: conf,
	}
	self.kvNodes[conf.Name] = n
	self.groups[raftConf.GroupID] = conf.Name
	self.mutex.Unlock()
	return nil
}

func (self *NamespaceMgr) GetNamespaceNode(ns string) *NamespaceNode {
	self.mutex.Lock()
	v, _ := self.kvNodes[ns]
	self.mutex.Unlock()
	return v
}

func (self *NamespaceMgr) GetNamespaceNodeFromGID(gid uint64) *NamespaceNode {
	self.mutex.Lock()
	gn, ok := self.groups[gid]
	if !ok {
		nodeLog.Errorf("group name not found %v ", gid)
		return nil
	}
	kv, ok := self.kvNodes[gn]
	self.mutex.Unlock()
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
	return nil
}

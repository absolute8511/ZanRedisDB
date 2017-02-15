package cluster

import (
	"bytes"
	"errors"
	"github.com/absolute8511/ZanRedisDB/node"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	MaxRetryWait = time.Second * 3
)

const (
	MAX_RAFT_JOIN_RUNNING = 3
)

func GetNamespacePartitionFileName(namespace string, partition int, suffix string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(namespace)
	tmpbuf.WriteString("-")
	tmpbuf.WriteString(strconv.Itoa(partition))
	tmpbuf.WriteString(suffix)
	return tmpbuf.String()
}

func GetNamespacePartitionBasePath(rootPath string, namespace string, partition int) string {
	return filepath.Join(rootPath, namespace)
}

type DataCoordinator struct {
	clusterKey       string
	register         DataNodeRegister
	pdMutex          sync.Mutex
	pdLeader         NodeInfo
	myNode           NodeInfo
	stopChan         chan struct{}
	tryCheckUnsynced chan bool
	wg               sync.WaitGroup
	stopping         int32
	catchupRunning   int32
	localNSMgr       *node.NamespaceMgr
}

func NewDataCoordinator(cluster string, nodeInfo *NodeInfo, nsMgr *node.NamespaceMgr) *DataCoordinator {
	coord := &DataCoordinator{
		clusterKey:       cluster,
		register:         nil,
		myNode:           *nodeInfo,
		stopChan:         make(chan struct{}),
		tryCheckUnsynced: make(chan bool, 1),
		localNSMgr:       nsMgr,
	}

	return coord
}

func (self *DataCoordinator) GetMyID() string {
	return self.myNode.GetID()
}

func (self *DataCoordinator) GetMyRegID() uint64 {
	return self.myNode.RegID
}

func (self *DataCoordinator) SetRegister(l DataNodeRegister) error {
	self.register = l
	if self.register != nil {
		self.register.InitClusterID(self.clusterKey)
		if self.myNode.RegID <= 0 {
			var err error
			self.myNode.RegID, err = self.register.NewRegisterNodeID()
			if err != nil {
				coordLog.Errorf("failed to init node register id: %v", err)
				return err
			}
			err = self.localNSMgr.SaveMachineRegID(self.myNode.RegID)
			if err != nil {
				coordLog.Errorf("failed to save register id: %v", err)
				return err
			}
		}
		self.myNode.ID = GenNodeID(&self.myNode, "pd")
		coordLog.Infof("node start with register id: %v", self.myNode.RegID)
	}
	return nil
}

func (self *DataCoordinator) Start() error {
	if self.register != nil {
		if self.myNode.RegID <= 0 {
			coordLog.Errorf("invalid register id: %v", self.myNode.RegID)
			return errors.New("invalid register id for data node")
		}
		err := self.register.Register(&self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register coordinator: %v", err)
			return err
		}
	}
	self.wg.Add(1)
	go self.watchPD()

	err := self.loadLocalNamespaceData()
	if err != nil {
		close(self.stopChan)
		return err
	}

	self.wg.Add(1)
	go self.checkForUnsyncedNamespaces()
	return nil
}

func (self *DataCoordinator) Stop() {
	if atomic.LoadInt32(&self.stopping) == 1 {
		return
	}
	self.prepareLeavingCluster()
	close(self.stopChan)
	self.wg.Wait()
}

func (self *DataCoordinator) GetCurrentPD() NodeInfo {
	self.pdMutex.Lock()
	defer self.pdMutex.Unlock()
	return self.pdLeader
}

func (self *DataCoordinator) GetAllPDNodes() ([]NodeInfo, error) {
	return self.register.GetAllPDNodes()
}

func (self *DataCoordinator) watchPD() {
	defer self.wg.Done()
	leaderChan := make(chan *NodeInfo, 1)
	if self.register != nil {
		go self.register.WatchPDLeader(leaderChan, self.stopChan)
	} else {
		return
	}
	for {
		select {
		case n, ok := <-leaderChan:
			if !ok {
				return
			}
			self.pdMutex.Lock()
			if n.GetID() != self.pdLeader.GetID() ||
				n.Epoch != self.pdLeader.Epoch {
				coordLog.Infof("pd leader changed: %v", n)
				self.pdLeader = *n
			}
			self.pdMutex.Unlock()
		}
	}
}

func (self *DataCoordinator) checkLocalNamespaceMagicCode(namespaceInfo *PartitionMetaInfo, tryFix bool) error {
	if namespaceInfo.MagicCode <= 0 {
		return nil
	}
	err := self.localNSMgr.CheckMagicCode(namespaceInfo.GetDesp(), namespaceInfo.MagicCode, tryFix)
	if err != nil {
		coordLog.Infof("namespace %v check magic code error: %v", namespaceInfo.GetDesp(), err)
		return err
	}
	return nil
}

func (self *DataCoordinator) forceCleanNamespaceData(nsInfo *PartitionMetaInfo) *CoordErr {
	// check if any data on local and try remove
	localErr := self.localNSMgr.ForceDeleteNamespaceData(nsInfo.GetDesp())
	if localErr != nil {
		if !os.IsNotExist(localErr) {
			coordLog.Infof("delete namespace %v local data failed : %v", nsInfo.GetDesp(), localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		}
	}
	return nil
}

func (self *DataCoordinator) ensureJoinNamespaceGroup(namespaceInfo *PartitionMetaInfo) *CoordErr {
	// check if in local raft group
	myRunning := atomic.AddInt32(&self.catchupRunning, 1)
	defer atomic.AddInt32(&self.catchupRunning, -1)
	if myRunning > MAX_RAFT_JOIN_RUNNING {
		coordLog.Infof("catching too much running: %v", myRunning)
		return ErrCatchupRunningBusy
	}

	coordLog.Infof("ensure local namespace %v join raft group", namespaceInfo.GetDesp())
	localNamespace := self.localNSMgr.GetNamespaceNode(namespaceInfo.GetDesp())
	if localNamespace == nil {
		coordLog.Errorf("join raft failed since the namespace %v is missing", namespaceInfo.GetDesp())
		return ErrNamespaceNotCreated
	}
	dyConf := &node.NamespaceDynamicConf{}
	localNamespace.SetDynamicInfo(*dyConf)
	if localNamespace.IsDataNeedFix() {
		// clean local data
	}
	localNamespace.SetDataFixState(false)
	// TODO: query raft member and check if myself is in the group
	// if not then ProposeConfChange and wait until raft member include myself
	coordLog.Infof("local namespace join done: %v", namespaceInfo.GetDesp())
	return nil

}

func (self *DataCoordinator) loadLocalNamespaceData() error {
	if self.localNSMgr == nil {
		return nil
	}
	namespaceMap, _, err := self.register.GetAllNamespaces()
	if err != nil {
		return err
	}
	for namespaceName, namespaceParts := range namespaceMap {
		for _, nsInfo := range namespaceParts {
			shouldLoad := FindSlice(nsInfo.RaftNodes, self.myNode.GetID()) != -1
			if !shouldLoad {
				if len(nsInfo.RaftNodes) >= nsInfo.Replica {
					self.forceCleanNamespaceData(&nsInfo)
				}
				continue
			}
			if _, err := self.getLocalExistNamespace(&nsInfo); err == nil {
				// already loaded
				self.ensureJoinNamespaceGroup(&nsInfo)
				continue
			}
			coordLog.Infof("init namespace: %v", nsInfo.GetDesp())
			if namespaceName == "" {
				continue
			}
			checkErr := self.checkLocalNamespaceMagicCode(&nsInfo, true)
			if checkErr != nil {
				coordLog.Errorf("failed to check namespace :%v, err:%v", nsInfo.GetDesp(), checkErr)
				continue
			}

			namespace, coordErr := self.updateLocalNamespace(&nsInfo)
			if coordErr != nil {
				coordLog.Errorf("failed to init/update local namespace %v: %v", nsInfo.GetDesp(), coordErr)
				panic(coordErr)
			}

			dyConf := &node.NamespaceDynamicConf{}
			namespace.SetDynamicInfo(*dyConf)
			localErr := self.checkAndFixLocalNamespaceData(&nsInfo, namespace)
			if localErr != nil {
				coordLog.Errorf("check local namespace %v data need to be fixed:%v", nsInfo.GetDesp(), localErr)
				namespace.SetDataFixState(true)
			}
			self.ensureJoinNamespaceGroup(&nsInfo)
		}
	}
	return nil
}

func (self *DataCoordinator) checkAndFixLocalNamespaceData(nsInfo *PartitionMetaInfo, localNamespace *node.NamespaceNode) error {
	return nil
}

func (self *DataCoordinator) getNamespaceRaftLeader(nsInfo *PartitionMetaInfo) string {
	return ""
}

func (self *DataCoordinator) transferMyNamespaceLeader(nsInfo *PartitionMetaInfo) {
}

func (self *DataCoordinator) checkForUnsyncedNamespaces() {
	ticker := time.NewTicker(time.Minute * 10)
	defer self.wg.Done()
	doWork := func() {
		// try load local namespace if any namespace raft group changed
		self.loadLocalNamespaceData()

		// check local namespaces with cluster to remove the unneed data
		tmpChecks := self.localNSMgr.GetNamespaces()
		for name, _ := range tmpChecks {
			splits := strings.SplitN(name, "-", 2)
			if len(splits) != 2 {
				coordLog.Warningf("namespace invalid: %v", name)
				continue
			}
			namespace := splits[0]
			pid, err := strconv.Atoi(splits[1])
			if err != nil {
				coordLog.Warningf("namespace invalid: %v, %v", name, splits)
				continue
			}
			namespaceMeta, err := self.register.GetNamespacePartInfo(namespace, pid)
			if err != nil {
				if err == ErrKeyNotFound {
					coordLog.Infof("the namespace should be clean since not found in register: %v", name)
				}
				continue
			}
			if FindSlice(namespaceMeta.RaftNodes, self.myNode.GetID()) == -1 {
				if len(namespaceMeta.RaftNodes) > 0 {
					coordLog.Infof("the namespace should be clean since not relevance to me: %v", namespaceMeta)
					self.removeLocalNamespace(namespaceMeta, true)
				}
			} else if len(namespaceMeta.RaftNodes) >= namespaceMeta.Replica {
				leader := self.getNamespaceRaftLeader(namespaceMeta)
				if leader == self.myNode.GetID() {
					if namespaceMeta.RaftNodes[0] != leader {
						// the raft leader check if I am the expected sharding leader,
						// if not, try to transfer the leader to expected node. We need do this
						// because we should make all the sharding leaders balanced on
						// all the cluster nodes.
						self.transferMyNamespaceLeader(namespaceMeta)
					}
				}
			}
		}
	}

	nsChangedChan := self.register.GetNamespacesNotifyChan()
	for {
		select {
		case <-self.stopChan:
			return
		case <-self.tryCheckUnsynced:
			doWork()
		case <-ticker.C:
			doWork()
		case <-nsChangedChan:
			doWork()
		}
	}
}

func (self *DataCoordinator) removeLocalNamespace(nsInfo *PartitionMetaInfo, removeData bool) *CoordErr {
	if removeData {
		coordLog.Infof("removing namespace : %v", nsInfo.GetDesp())
		// check if any data on local and try remove
		self.forceCleanNamespaceData(nsInfo)
	} else {
		localNamespace := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
		if localNamespace == nil {
			return ErrNamespaceNotCreated
		}
		localNamespace.Close()
	}
	return nil
}

func (self *DataCoordinator) getLocalExistNamespace(namespaceInfo *PartitionMetaInfo) (*node.NamespaceNode, *CoordErr) {
	return nil, nil
}

func (self *DataCoordinator) getRaftAddrForNode(nid string) (string, *CoordErr) {
	node, err := self.register.GetNodeInfo(nid)
	if err != nil {
		return "", &CoordErr{err.Error(), RpcNoErr, CoordRegisterErr}
	}
	return node.RaftTransportAddr, nil
}

func (self *DataCoordinator) updateLocalNamespace(namespaceInfo *PartitionMetaInfo) (*node.NamespaceNode, *CoordErr) {
	// check namespace exist and prepare on local.
	raftID, ok := namespaceInfo.RaftIDs[self.GetMyID()]
	if !ok {
		return nil, ErrNamespaceConfInvalid
	}
	var err *CoordErr
	nsConf := node.NewNSConfig()
	nsConf.Name = namespaceInfo.GetDesp()
	nsConf.EngType = namespaceInfo.EngType
	nsConf.PartitionNum = namespaceInfo.PartitionNum
	nsConf.RaftGroupConf.GroupID = uint64(namespaceInfo.MinGID) + uint64(namespaceInfo.Partition)
	nsConf.RaftGroupConf.SeedNodes = make([]node.ReplicaInfo, 0)
	for _, nid := range namespaceInfo.RaftNodes {
		if nid == self.GetMyID() {
			continue
		}
		var rinfo node.ReplicaInfo
		rinfo.NodeID = ExtractRegIDFromGenID(nid)
		rid, ok := namespaceInfo.RaftIDs[nid]
		if !ok {
			continue
		}
		rinfo.ReplicaID = rid
		rinfo.RaftAddr, err = self.getRaftAddrForNode(nid)
		if err != nil {
			coordLog.Infof("can not found raft address for node: %v, %v", nid, err)
			continue
		}
		nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, rinfo)
	}
	if len(namespaceInfo.RaftNodes) == 1 && len(nsConf.RaftGroupConf.SeedNodes) == 0 {
		var rinfo node.ReplicaInfo
		rinfo.NodeID = ExtractRegIDFromGenID(self.GetMyID())
		rinfo.ReplicaID = raftID
		rinfo.RaftAddr = self.myNode.RaftTransportAddr
		nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, rinfo)
	}
	if len(nsConf.RaftGroupConf.SeedNodes) == 0 {
		coordLog.Warningf("can not found any seed nodes for namespace: %v", namespaceInfo)
		return nil, ErrNamespaceConfInvalid
	}

	localNode, _ := self.localNSMgr.InitNamespaceNode(nsConf, raftID)
	if localNode != nil {
		if checkErr := localNode.CheckRaftConf(raftID, nsConf); checkErr != nil {
			coordLog.Infof("local namespace %v mismatch with the new raft config removing: %v", namespaceInfo.GetDesp(), checkErr)
			self.localNSMgr.ForceDeleteNamespaceData(nsConf.Name)
			localNode, _ = self.localNSMgr.InitNamespaceNode(nsConf, raftID)
		}
	}
	if localNode == nil {
		return nil, ErrLocalInitNamespaceFailed
	}

	localErr := localNode.SetMagicCode(namespaceInfo.MagicCode)
	if localErr != nil {
		return localNode, ErrLocalInitNamespaceFailed
	}
	dyConf := &node.NamespaceDynamicConf{}
	localNode.SetDynamicInfo(*dyConf)
	return localNode, nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *DataCoordinator) prepareLeavingCluster() {
	coordLog.Infof("I am prepare leaving the cluster.")
	allNamespaces, _, _ := self.register.GetAllNamespaces()
	for _, nsParts := range allNamespaces {
		for _, nsInfo := range nsParts {
			if FindSlice(nsInfo.RaftNodes, self.myNode.GetID()) == -1 {
				continue
			}
			localNamespace := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			if localNamespace == nil {
				continue
			}
			if len(nsInfo.RaftNodes)-1 <= nsInfo.Replica/2 {
				coordLog.Infof("The isr nodes in namespace %v is not enough while leaving: %v",
					nsInfo.GetDesp(), nsInfo.RaftNodes)
			}
		}
	}
	coordLog.Infof("prepare leaving finished.")
	self.localNSMgr.Stop()
	if self.register != nil {
		atomic.StoreInt32(&self.stopping, 1)
		self.register.Unregister(&self.myNode)
	}
}

func (self *DataCoordinator) Stats(namespace string, part int) *CoordStats {
	s := &CoordStats{}
	s.NsCoordStats = make([]NamespaceCoordStat, 0)
	if len(namespace) > 0 {
		meta, err := self.register.GetNamespaceMetaInfo(namespace)
		if err != nil {
			coordLog.Infof("failed to get namespace info: %v", err)
			return s
		}
		if part >= 0 {
			nsInfo, err := self.register.GetNamespacePartInfo(namespace, part)
			if err != nil {
			} else {
				var stat NamespaceCoordStat
				stat.Name = namespace
				stat.Partition = part
				for _, nid := range nsInfo.RaftNodes {
					stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
				}
				s.NsCoordStats = append(s.NsCoordStats, stat)
			}
		} else {
			for i := 0; i < meta.PartitionNum; i++ {
				nsInfo, err := self.register.GetNamespacePartInfo(namespace, part)
				if err != nil {
					continue
				}
				var stat NamespaceCoordStat
				stat.Name = namespace
				stat.Partition = nsInfo.Partition
				for _, nid := range nsInfo.RaftNodes {
					stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
				}
				s.NsCoordStats = append(s.NsCoordStats, stat)
			}
		}
	}
	return s
}

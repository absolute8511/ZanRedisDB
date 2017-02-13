package cluster

import (
	"bytes"
	"github.com/absolute8511/ZanRedisDB/node"
	"os"
	"path/filepath"
	"strconv"
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
	namespaceCoords  map[string]map[int]*NamespaceCoordinator
	coordMutex       sync.RWMutex
	myNode           NodeInfo
	flushNotifyChan  chan NamespaceNameInfo
	stopChan         chan struct{}
	dataRootPath     string
	tryCheckUnsynced chan bool
	wg               sync.WaitGroup
	enableBenchCost  bool
	stopping         int32
	catchupRunning   int32
	localNSMgr       *node.NamespaceMgr
}

func NewDataCoordinator(cluster string, nodeInfo *NodeInfo) *DataCoordinator {
	coord := &DataCoordinator{
		clusterKey:       cluster,
		register:         nil,
		namespaceCoords:  make(map[string]map[int]*NamespaceCoordinator),
		myNode:           *nodeInfo,
		stopChan:         make(chan struct{}),
		tryCheckUnsynced: make(chan bool, 1),
	}

	return coord
}

func (self *DataCoordinator) GetMyID() string {
	return self.myNode.GetID()
}

func (self *DataCoordinator) SetRegister(l DataNodeRegister) {
	self.register = l
	if self.register != nil {
		self.register.InitClusterID(self.clusterKey)
	}
}

func (self *DataCoordinator) Start() error {
	self.wg.Add(1)
	go self.watchPD()

	err := self.loadLocalNamespaceData()
	if err != nil {
		close(self.stopChan)
		return err
	}
	if self.register != nil {
		err := self.register.Register(&self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register coordinator: %v", err)
			return err
		}
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

func (self *DataCoordinator) forceCleanNamespaceData(namespaceName string, partition int) *CoordErr {
	// check if any data on local and try remove
	localErr := self.localNSMgr.ForceDeleteNamespaceData(GetDesp(namespaceName, partition))
	if localErr != nil {
		if !os.IsNotExist(localErr) {
			coordLog.Infof("delete namespace %v-%v local data failed : %v", namespaceName, partition, localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		}
	}
	return nil
}

func (self *DataCoordinator) ensureJoinNamespaceGroup(namespaceInfo *PartitionMetaInfo) *CoordErr {
	// check if in local raft group
	tc, coordErr := self.getNamespaceCoord(namespaceInfo.Name, namespaceInfo.Partition)
	if coordErr != nil {
		coordLog.Warningf("namespace(%v) catching failed since namespace coordinator missing: %v", namespaceInfo.Name, coordErr)
		return ErrMissingNamespaceCoord
	}
	if !atomic.CompareAndSwapInt32(&tc.catchupRunning, 0, 1) {
		coordLog.Infof("namespace(%v) catching already running", namespaceInfo.Name)
		return ErrNamespaceCatchupAlreadyRunning
	}
	defer atomic.StoreInt32(&tc.catchupRunning, 0)
	myRunning := atomic.AddInt32(&self.catchupRunning, 1)
	defer atomic.AddInt32(&self.catchupRunning, -1)
	if myRunning > MAX_RAFT_JOIN_RUNNING {
		coordLog.Infof("catching too much running: %v", myRunning)
		return ErrCatchupRunningBusy
	}

	coordLog.Infof("local namespace join raft group : %v", namespaceInfo.GetDesp())

	tc.writeHold.Lock()
	defer tc.writeHold.Unlock()
	if tc.IsExiting() {
		coordLog.Warningf("catchup exit since the namespace is exited")
		return ErrNamespaceExiting
	}
	localNamespace := self.localNSMgr.GetNamespaceNode(namespaceInfo.GetDesp())
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
					self.forceCleanNamespaceData(nsInfo.Name, nsInfo.Partition)
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

			//tc, err := NewNamespaceCoordinator(namespaceName, partition)
			//if err != nil {
			//	coordLog.Infof("failed to get namespace coordinator:%v-%v, err:%v", namespaceName, partition, err)
			//	continue
			//}
			//tc.namespaceInfo = nsInfo
			//self.coordMutex.Lock()
			//coords, ok := self.namespaceCoords[namespaceName]
			//if !ok {
			//	coords = make(map[int]*NamespaceCoordinator)
			//	self.namespaceCoords[namespaceName] = coords
			//}
			//coords[nsInfo.Partition] = tc
			//self.coordMutex.Unlock()

			//tc.writeHold.Lock()
			namespace, coordErr := self.updateLocalNamespace(&nsInfo)
			if coordErr != nil {
				coordLog.Errorf("failed to init/update local namespace %v: %v", nsInfo.GetDesp(), coordErr)
				panic(coordErr)
			}
			//tc.writeHold.Unlock()

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

		// check coordinator with cluster
		tmpChecks := make(map[string]map[int]bool, len(self.namespaceCoords))
		self.coordMutex.Lock()
		for namespace, info := range self.namespaceCoords {
			for pid, tc := range info {
				if tc.IsExiting() {
					continue
				}
				if _, ok := tmpChecks[namespace]; !ok {
					tmpChecks[namespace] = make(map[int]bool)
				}
				tmpChecks[namespace][pid] = true
			}
		}
		self.coordMutex.Unlock()
		for namespace, info := range tmpChecks {
			for pid, _ := range info {
				namespaceMeta, err := self.register.GetNamespacePartInfo(namespace, pid)
				if err != nil {
					continue
				}
				if FindSlice(namespaceMeta.RaftNodes, self.myNode.GetID()) == -1 {
					if len(namespaceMeta.RaftNodes) >= namespaceMeta.Replica {
						coordLog.Infof("the namespace should be clean since not relevance to me: %v", namespaceMeta)
						self.removeNamespaceCoord(namespaceMeta.Name, namespaceMeta.Partition, true)
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

func (self *DataCoordinator) updateNamespaceInfo(namespaceCoord *NamespaceCoordinator, newNamespaceInfo *PartitionMetaInfo) *CoordErr {
	if atomic.LoadInt32(&self.stopping) == 1 {
		return ErrClusterChanged
	}
	oldData := namespaceCoord.GetData()
	if FindSlice(oldData.namespaceInfo.RaftNodes, self.myNode.GetID()) == -1 &&
		FindSlice(newNamespaceInfo.RaftNodes, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am notified to be a new node in ISR: %v", self.myNode.GetID())
	}
	if namespaceCoord.IsExiting() {
		coordLog.Infof("update the namespace info: %v while exiting.", oldData.namespaceInfo.GetDesp())
		return nil
	}
	if newNamespaceInfo.PartitionReplicaInfo.Epoch < oldData.namespaceInfo.PartitionReplicaInfo.Epoch {
		return ErrEpochLessThanCurrent
	}

	namespaceCoord.dataMutex.Lock()
	coordLog.Infof("update the namespace info: %v", namespaceCoord.namespaceInfo.GetDesp())
	newCoordData := namespaceCoord.coordData.GetCopy()
	if namespaceCoord.namespaceInfo.Epoch != newNamespaceInfo.Epoch {
		newCoordData.namespaceInfo = *newNamespaceInfo
	}
	namespaceCoord.coordData = newCoordData
	namespaceCoord.dataMutex.Unlock()

	_, err := self.updateLocalNamespace(newNamespaceInfo)
	if err != nil {
		coordLog.Warningf("init local namespace failed: %v", err)
		self.removeNamespaceCoord(newNamespaceInfo.Name, newNamespaceInfo.Partition, false)
		return err
	}

	if FindSlice(newNamespaceInfo.RaftNodes, self.myNode.GetID()) != -1 {
		coordLog.Infof("I am notified to join raft group list: %v", newNamespaceInfo.GetDesp())
		self.ensureJoinNamespaceGroup(newNamespaceInfo)
	}
	return nil
}

func (self *DataCoordinator) getNamespaceCoordData(namespace string, partition int) (*coordData, *CoordErr) {
	c, err := self.getNamespaceCoord(namespace, partition)
	if err != nil {
		return nil, err
	}
	return c.GetData(), nil
}

// any modify operation on the namespace should check for namespace leader.
func (self *DataCoordinator) getNamespaceCoord(namespace string, partition int) (*NamespaceCoordinator, *CoordErr) {
	self.coordMutex.RLock()
	defer self.coordMutex.RUnlock()
	if v, ok := self.namespaceCoords[namespace]; ok {
		if namespaceCoord, ok := v[partition]; ok {
			return namespaceCoord, nil
		}
	}
	coordErrStats.incNamespaceCoordMissingErr()
	return nil, ErrMissingNamespaceCoord
}

func (self *DataCoordinator) removeNamespaceCoord(namespace string, partition int, removeData bool) (*NamespaceCoordinator, *CoordErr) {
	var namespaceCoord *NamespaceCoordinator
	self.coordMutex.Lock()
	defer self.coordMutex.Unlock()
	if v, ok := self.namespaceCoords[namespace]; ok {
		if tc, ok := v[partition]; ok {
			namespaceCoord = tc
			delete(v, partition)
		}
	}
	var err *CoordErr
	if namespaceCoord == nil {
		err = ErrMissingNamespaceCoord
	} else {
		coordLog.Infof("removing namespace coodinator: %v-%v", namespace, partition)
		namespaceCoord.Delete(removeData)
		err = nil
	}
	if removeData {
		coordLog.Infof("removing namespace data: %v-%v", namespace, partition)
		// check if any data on local and try remove
		self.forceCleanNamespaceData(namespace, partition)
	}
	return namespaceCoord, err
}

func (self *DataCoordinator) getLocalExistNamespace(namespaceInfo *PartitionMetaInfo) (*node.NamespaceNode, *CoordErr) {
	return nil, nil
}

func (self *DataCoordinator) updateLocalNamespace(namespaceInfo *PartitionMetaInfo) (*node.NamespaceNode, *CoordErr) {
	// check namespace exist and prepare on local.
	t := self.localNSMgr.GetNamespaceNode(namespaceInfo.GetDesp())
	if t == nil {
		return nil, ErrLocalInitNamespaceFailed
	}
	localErr := self.localNSMgr.SetNamespaceMagicCode(t, namespaceInfo.MagicCode)
	if localErr != nil {
		return t, ErrLocalInitNamespaceFailed
	}
	dyConf := &node.NamespaceDynamicConf{}
	t.SetDynamicInfo(*dyConf)
	return t, nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *DataCoordinator) prepareLeavingCluster() {
	coordLog.Infof("I am prepare leaving the cluster.")
	tmpNamespaceCoords := make(map[string]map[int]*NamespaceCoordinator, len(self.namespaceCoords))
	self.coordMutex.RLock()
	for t, v := range self.namespaceCoords {
		tmp, ok := tmpNamespaceCoords[t]
		if !ok {
			tmp = make(map[int]*NamespaceCoordinator)
			tmpNamespaceCoords[t] = tmp
		}
		for pid, coord := range v {
			tmp[pid] = coord
		}
	}
	self.coordMutex.RUnlock()
	for _, namespaceData := range tmpNamespaceCoords {
		for _, tpCoord := range namespaceData {
			tcData := tpCoord.GetData()
			localNamespace := self.localNSMgr.GetNamespaceNode(tpCoord.namespaceInfo.GetDesp())
			if localNamespace == nil {
				coordLog.Infof("no local namespace")
				continue
			}

			if FindSlice(tcData.namespaceInfo.RaftNodes, self.myNode.GetID()) == -1 {
				tpCoord.Exiting()
				continue
			}
			if len(tcData.namespaceInfo.RaftNodes)-1 <= tcData.namespaceInfo.Replica/2 {
				coordLog.Infof("The isr nodes in namespace %v is not enough while leaving: %v",
					tpCoord.namespaceInfo.GetDesp(), tpCoord.namespaceInfo.RaftNodes)
			}

			tpCoord.Exiting()
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
		if part >= 0 {
			tcData, err := self.getNamespaceCoordData(namespace, part)
			if err != nil {
			} else {
				var stat NamespaceCoordStat
				stat.Name = namespace
				stat.Partition = part
				for _, nid := range tcData.namespaceInfo.RaftNodes {
					stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
				}
				s.NsCoordStats = append(s.NsCoordStats, stat)
			}
		} else {
			self.coordMutex.RLock()
			defer self.coordMutex.RUnlock()
			v, ok := self.namespaceCoords[namespace]
			if ok {
				for _, tc := range v {
					var stat NamespaceCoordStat
					stat.Name = namespace
					stat.Partition = tc.namespaceInfo.Partition
					for _, nid := range tc.namespaceInfo.RaftNodes {
						stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
					}
					s.NsCoordStats = append(s.NsCoordStats, stat)
				}
			}
		}
	}
	return s
}

package datanode_coord

import (
	"bytes"
	"encoding/json"
	"errors"
	. "github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	node "github.com/absolute8511/ZanRedisDB/node"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	MaxRetryWait         = time.Second * 3
	ErrNamespaceNotReady = NewCoordErr("namespace node is not ready", CoordLocalErr)
	ErrNamespaceInvalid  = errors.New("namespace name is invalid")
	ErrNamespaceNotFound = errors.New("namespace is not found")
)

const (
	MAX_RAFT_JOIN_RUNNING = 5
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
				CoordLog().Errorf("failed to init node register id: %v", err)
				return err
			}
			err = self.localNSMgr.SaveMachineRegID(self.myNode.RegID)
			if err != nil {
				CoordLog().Errorf("failed to save register id: %v", err)
				return err
			}
		}
		self.myNode.ID = GenNodeID(&self.myNode, "datanode")
		CoordLog().Infof("node start with register id: %v", self.myNode.RegID)
	}
	return nil
}

func (self *DataCoordinator) Start() error {
	if self.register != nil {
		if self.myNode.RegID <= 0 {
			CoordLog().Errorf("invalid register id: %v", self.myNode.RegID)
			return errors.New("invalid register id for data node")
		}
		err := self.register.Register(&self.myNode)
		if err != nil {
			CoordLog().Warningf("failed to register coordinator: %v", err)
			return err
		}
	}
	if self.localNSMgr != nil {
		self.localNSMgr.Start()
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
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
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
			if n.GetID() != self.pdLeader.GetID() {
				CoordLog().Infof("pd leader changed from %v to %v", self.pdLeader, n)
				self.pdLeader = *n
			}
			self.pdMutex.Unlock()
		}
	}
}

func (self *DataCoordinator) checkLocalNamespaceMagicCode(nsInfo *PartitionMetaInfo, tryFix bool) error {
	if nsInfo.MagicCode <= 0 {
		return nil
	}
	err := self.localNSMgr.CheckMagicCode(nsInfo.GetDesp(), nsInfo.MagicCode, tryFix)
	if err != nil {
		CoordLog().Infof("namespace %v check magic code error: %v", nsInfo.GetDesp(), err)
		return err
	}
	return nil
}

type PartitionList []PartitionMetaInfo

func (self PartitionList) Len() int { return len(self) }
func (self PartitionList) Less(i, j int) bool {
	return self[i].Partition < self[j].Partition
}
func (self PartitionList) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self *DataCoordinator) loadLocalNamespaceData() error {
	if self.localNSMgr == nil {
		return nil
	}
	namespaceMap, _, err := self.register.GetAllNamespaces()
	if err != nil {
		if err == ErrKeyNotFound {
			return nil
		}
		return err
	}
	sortedParts := make(PartitionList, 0)
	for namespaceName, namespaceParts := range namespaceMap {
		sortedParts = sortedParts[:0]
		for _, part := range namespaceParts {
			sortedParts = append(sortedParts, part)
		}
		sort.Sort(sortedParts)
		for _, nsInfo := range sortedParts {
			localNamespace := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			shouldLoad := self.isNamespaceShouldStart(nsInfo)
			if !shouldLoad {
				if len(nsInfo.GetISR()) >= nsInfo.Replica && localNamespace != nil {
					self.removeLocalNamespaceFromRaft(localNamespace, false)
				}
				continue
			}
			if localNamespace != nil {
				// already loaded
				joinErr := self.ensureJoinNamespaceGroup(nsInfo, localNamespace, false)
				if joinErr != nil && joinErr != ErrNamespaceConfInvalid {
					// we ensure join group as order for partitions
					break
				}
				continue
			}
			CoordLog().Infof("loading namespace: %v", nsInfo.GetDesp())
			if namespaceName == "" {
				continue
			}
			checkErr := self.checkLocalNamespaceMagicCode(&nsInfo, true)
			if checkErr != nil {
				CoordLog().Errorf("failed to check namespace :%v, err:%v", nsInfo.GetDesp(), checkErr)
				continue
			}

			localNamespace, coordErr := self.updateLocalNamespace(&nsInfo, false)
			if coordErr != nil {
				CoordLog().Errorf("failed to init/update local namespace %v: %v", nsInfo.GetDesp(), coordErr)
				continue
			}

			dyConf := &node.NamespaceDynamicConf{}
			localNamespace.SetDynamicInfo(*dyConf)
			localErr := self.checkAndFixLocalNamespaceData(&nsInfo, localNamespace)
			if localErr != nil {
				CoordLog().Errorf("check local namespace %v data need to be fixed:%v", nsInfo.GetDesp(), localErr)
				localNamespace.SetDataFixState(true)
			}
			joinErr := self.ensureJoinNamespaceGroup(nsInfo, localNamespace, true)
			if joinErr != nil && joinErr != ErrNamespaceConfInvalid {
				// we ensure join group as order for partitions
				break
			}
		}
	}
	return nil
}

func (self *DataCoordinator) isMeInRaftGroup(nsInfo *PartitionMetaInfo) (bool, error) {
	var lastErr error
	for _, remoteNode := range nsInfo.GetISR() {
		if remoteNode == self.GetMyID() {
			continue
		}
		nip, _, _, httpPort := ExtractNodeInfoFromID(remoteNode)
		var rsp []*common.MemberInfo
		code, err := common.APIRequest("GET",
			"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetMembers+"/"+nsInfo.GetDesp(),
			nil, time.Second*3, &rsp)
		if err != nil {
			CoordLog().Infof("failed to get members from %v for namespace: %v, %v", nip, nsInfo.GetDesp(), err)
			if code == 404 {
				lastErr = ErrNamespaceNotFound
			} else {
				lastErr = err
			}
			continue
		}

		for _, m := range rsp {
			if m.NodeID == self.GetMyRegID() && m.ID == nsInfo.RaftIDs[self.GetMyID()] {
				CoordLog().Infof("from %v for namespace: %v, node is still in raft: %v", nip, nsInfo.GetDesp(), m)
				return true, nil
			}
		}
	}
	return false, lastErr
}

func (self *DataCoordinator) isNamespaceShouldStart(nsInfo PartitionMetaInfo) bool {
	// it may happen that the node marked as removing can not be removed because
	// the raft group has not enough quorum to do the proposal to change the configure.
	// In this way we need start the removing node to join the raft group and then
	// the leader can remove the node from the raft group, and then we can safely remove
	// the removing node finally
	shouldLoad := FindSlice(nsInfo.RaftNodes, self.GetMyID()) != -1
	if !shouldLoad {
		return false
	}
	rm, ok := nsInfo.Removings[self.GetMyID()]
	if !ok {
		return true
	}
	if rm.RemoveReplicaID != nsInfo.RaftIDs[self.GetMyID()] {
		return true
	}

	inRaft, err := self.isMeInRaftGroup(&nsInfo)
	if inRaft || err == ErrNamespaceNotFound {
		CoordLog().Infof("removing node %v-%v should join namespace %v since still in raft",
			self.GetMyID(), rm.RemoveReplicaID, nsInfo.GetDesp())
		return true
	}
	return false
}

func (self *DataCoordinator) isNamespaceShouldStop(nsInfo PartitionMetaInfo, localNamespace *node.NamespaceNode) bool {
	// removing node can stop local raft only when all the other members
	// are notified to remove this node
	// Mostly, the remove node proposal will handle the raft node stop, however
	// there are some situations to be checked here.
	inMeta := false
	for _, nid := range nsInfo.RaftNodes {
		if nid == self.GetMyID() {
			replicaID := nsInfo.RaftIDs[nid]
			if replicaID == localNamespace.GetRaftID() {
				inMeta = true
				break
			}
		}
	}
	var rmID uint64
	if inMeta {
		rm, ok := nsInfo.Removings[self.GetMyID()]
		if !ok {
			return false
		}
		rmID = rm.RemoveReplicaID
		if rm.RemoveReplicaID != nsInfo.RaftIDs[self.GetMyID()] {
			return false
		}
	} else {
		CoordLog().Infof("no any meta info for this namespace: %v", nsInfo.GetDesp(), localNamespace.GetRaftID())
	}

	inRaft, err := self.isMeInRaftGroup(&nsInfo)
	if inRaft || err != nil {
		return false
	}
	mems := localNamespace.GetMembers()
	for _, m := range mems {
		if m.ID == rmID {
			return false
		}
	}
	CoordLog().Infof("removing node %v-%v should stop namespace %v (replica %v) since not in any raft group anymore",
		self.GetMyID(), rmID, nsInfo.GetDesp(), localNamespace.GetRaftID())
	return true
}

func (self *DataCoordinator) checkAndFixLocalNamespaceData(nsInfo *PartitionMetaInfo, localNamespace *node.NamespaceNode) error {
	return nil
}

func (self *DataCoordinator) addNamespaceRaftMember(nsInfo *PartitionMetaInfo, m *common.MemberInfo) {
	for nid, removing := range nsInfo.Removings {
		if m.ID == removing.RemoveReplicaID && m.NodeID == ExtractRegIDFromGenID(nid) {
			CoordLog().Infof("raft member %v is marked as removing in meta: %v, ignore add raft member", m, nsInfo.Removings)
			return
		}
	}
	nsNode := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		CoordLog().Infof("namespace %v not found while add member", nsInfo.GetDesp())
		return
	}
	err := nsNode.Node.ProposeAddMember(*m)
	if err != nil {
		CoordLog().Infof("%v propose add %v failed: %v", nsInfo.GetDesp(), m, err)
	} else {
		CoordLog().Infof("namespace %v propose add member %v", nsInfo.GetDesp(), m)
	}
}

func (self *DataCoordinator) removeNamespaceRaftMember(nsInfo *PartitionMetaInfo, m *common.MemberInfo) {
	nsNode := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		CoordLog().Infof("namespace %v not found while remove member", nsInfo.GetDesp())
		return
	}

	err := nsNode.Node.ProposeRemoveMember(*m)
	if err != nil {
		CoordLog().Infof("propose remove %v failed: %v", m, err)
	} else {
		CoordLog().Infof("namespace %v propose remove member %v", nsInfo.GetDesp(), m)
	}
}

func (self *DataCoordinator) getNamespaceRaftMembers(nsInfo *PartitionMetaInfo) []*common.MemberInfo {
	nsNode := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return nil
	}
	return nsNode.Node.GetMembers()
}

func (self *DataCoordinator) getNamespaceRaftLeader(nsInfo *PartitionMetaInfo) uint64 {
	nsNode := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return 0
	}
	m := nsNode.Node.GetLeadMember()
	if m == nil {
		return 0
	}
	return m.NodeID
}

func (self *DataCoordinator) transferMyNamespaceLeader(nsInfo *PartitionMetaInfo, nid string) {
	nsNode := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return
	}
	toRaftID, ok := nsInfo.RaftIDs[nid]
	if !ok {
		CoordLog().Warningf("transfer namespace %v leader to %v failed for missing raft id: %v",
			nsInfo.GetDesp(), nid, nsInfo.RaftIDs)
		return
	}
	CoordLog().Infof("begin transfer namespace %v leader to %v", nsInfo.GetDesp(), nid)
	err := nsNode.TransferMyLeader(ExtractRegIDFromGenID(nid), toRaftID)
	if err != nil {
		CoordLog().Infof("failed to transfer namespace %v leader to %v: %v", nsInfo.GetDesp(), nid, err)
	}
}

func (self *DataCoordinator) checkForUnsyncedNamespaces() {
	ticker := time.NewTicker(time.Minute * 5)
	defer self.wg.Done()
	type pendingRemoveInfo struct {
		ts time.Time
		m  common.MemberInfo
	}
	pendingRemovings := make(map[string]map[uint64]pendingRemoveInfo)
	doWork := func() {
		if atomic.LoadInt32(&self.stopping) == 1 {
			return
		}
		CoordLog().Debugf("check for namespace sync...")
		// try load local namespace if any namespace raft group changed
		self.loadLocalNamespaceData()

		// check local namespaces with cluster to remove the unneed data
		tmpChecks := self.localNSMgr.GetNamespaces()
		for name, localNamespace := range tmpChecks {
			namespace, pid := common.GetNamespaceAndPartition(name)
			if namespace == "" {
				CoordLog().Warningf("namespace invalid: %v", name)
				continue
			}
			namespaceMeta, err := self.register.GetNamespacePartInfo(namespace, pid)
			if err != nil {
				if err == ErrKeyNotFound {
					CoordLog().Infof("the namespace should be clean since not found in register: %v", name)
					_, err = self.register.GetNamespaceMetaInfo(namespace)
					if err == ErrKeyNotFound {
						self.forceRemoveLocalNamespace(localNamespace)
					}
				}
				CoordLog().Infof("got namespace %v meta failed: %v", name, err)
				go self.tryCheckNamespaces()
				continue
			}
			if self.isNamespaceShouldStop(*namespaceMeta, localNamespace) {
				self.forceRemoveLocalNamespace(localNamespace)
				continue
			}
			leader := self.getNamespaceRaftLeader(namespaceMeta)
			if leader == 0 {
				continue
			}
			isrList := namespaceMeta.GetISR()
			localRID := localNamespace.GetRaftID()

			if localRID != namespaceMeta.RaftIDs[self.myNode.GetID()] {
				if len(isrList) > 0 {
					CoordLog().Infof("the namespace should be clean : %v", namespaceMeta)
					self.removeLocalNamespaceFromRaft(localNamespace, true)
				}
				continue
			}
			// only leader check the follower status
			if leader != self.GetMyRegID() || len(isrList) == 0 {
				continue
			}
			isReplicasEnough := len(isrList) >= namespaceMeta.Replica
			members := self.getNamespaceRaftMembers(namespaceMeta)
			if len(members) < namespaceMeta.Replica {
				isReplicasEnough = false
			}

			if FindSlice(isrList, self.GetMyID()) == -1 {
				CoordLog().Infof("namespace %v leader is not in isr: %v, maybe removing",
					namespaceMeta.GetDesp(), isrList)
				if time.Now().UnixNano()-localNamespace.GetLastLeaderChangedTime() > time.Minute.Nanoseconds() {
					self.transferMyNamespaceLeader(namespaceMeta, isrList[0])
				}
				continue
			}
			if isReplicasEnough && isrList[0] != self.GetMyID() {
				// the raft leader check if I am the expected sharding leader,
				// if not, try to transfer the leader to expected node. We need do this
				// because we should make all the sharding leaders balanced on
				// all the cluster nodes.
				// avoid transfer while some node is leaving, so wait enough time to
				// allow node leaving
				if time.Now().UnixNano()-localNamespace.GetLastLeaderChangedTime() > time.Minute.Nanoseconds() {
					self.transferMyNamespaceLeader(namespaceMeta, isrList[0])
				}
			} else {
				// check if any replica is not joined to members
				anyWaitingJoin := false
				for nid, rid := range namespaceMeta.RaftIDs {
					if _, ok := namespaceMeta.Removings[nid]; ok {
						continue
					}
					found := false
					for _, m := range members {
						if m.ID == rid {
							found = true
							if m.NodeID != ExtractRegIDFromGenID(nid) {
								CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
							}
							break
						}
					}
					if !found {
						anyWaitingJoin = true
						CoordLog().Infof("namespace %v new node still waiting join raft : %v, %v", namespaceMeta.GetDesp(), rid, nid)
					}
				}
				if anyWaitingJoin || len(members) < len(isrList) {
					go self.tryCheckNamespaces()
					continue
				}
				// the members is more than replica, we need to remove the member that is not necessary anymore
				pendings, ok := pendingRemovings[namespaceMeta.GetDesp()]
				if !ok {
					pendings = make(map[uint64]pendingRemoveInfo)
					pendingRemovings[namespaceMeta.GetDesp()] = pendings
				}
				newestReplicaInfo, err := self.register.GetRemoteNamespaceReplicaInfo(namespaceMeta.Name, namespaceMeta.Partition)
				if err != nil {
					if err != ErrKeyNotFound {
						go self.tryCheckNamespaces()
					}
					delete(pendingRemovings, namespaceMeta.GetDesp())
					continue
				}
				namespaceMeta.PartitionReplicaInfo = *newestReplicaInfo
				for _, m := range members {
					found := false
					for nid, rid := range newestReplicaInfo.RaftIDs {
						if m.ID == rid {
							found = true
							if m.NodeID != ExtractRegIDFromGenID(nid) {
								CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
							}
							break
						}
					}
					if !found {
						CoordLog().Infof("raft member %v not found in meta: %v", m, newestReplicaInfo.RaftNodes)
						// here we do not remove other member immediately from raft
						// it may happen while the namespace info in the register is not updated due to network lag
						// so the new added node (add by api) may not in the meta info
						if pendRemove, ok := pendings[m.ID]; ok {
							if !pendRemove.m.IsEqual(m) {
								pendings[m.ID] = pendingRemoveInfo{
									ts: time.Now(),
									m:  *m,
								}
							} else if time.Since(pendRemove.ts) > time.Minute {
								CoordLog().Infof("pending removing member %v finally removed since not in meta", pendRemove)
								self.removeNamespaceRaftMember(namespaceMeta, m)
							}
						} else {
							pendings[m.ID] = pendingRemoveInfo{
								ts: time.Now(),
								m:  *m,
							}
						}
						go self.tryCheckNamespaces()
					} else {
						delete(pendings, m.ID)
						for nid, removing := range newestReplicaInfo.Removings {
							if m.ID == removing.RemoveReplicaID && m.NodeID == ExtractRegIDFromGenID(nid) {
								CoordLog().Infof("raft member %v is marked as removing in meta: %v", m, newestReplicaInfo.Removings)
								self.removeNamespaceRaftMember(namespaceMeta, m)
							}
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
			CoordLog().Infof("trigger check by namespace changed")
			doWork()
		}
	}
}

func (self *DataCoordinator) forceRemoveLocalNamespace(localNamespace *node.NamespaceNode) {
	err := localNamespace.Destroy()
	if err != nil {
		CoordLog().Infof("failed to force remove local data: %v", err)
	}
}

func (self *DataCoordinator) removeLocalNamespaceFromRaft(localNamespace *node.NamespaceNode, removeFromRaft bool) *CoordErr {
	if removeFromRaft {
		if !localNamespace.IsReady() {
			return ErrNamespaceNotReady
		}
		m := localNamespace.Node.GetLocalMemberInfo()
		CoordLog().Infof("propose remove %v from namespace : %v", m.ID, m.GroupName)

		localErr := localNamespace.Node.ProposeRemoveMember(*m)
		if localErr != nil {
			CoordLog().Infof("propose remove self %v failed : %v", m, localErr)
			return &CoordErr{ErrMsg: localErr.Error(), ErrCode: RpcCommonErr, ErrType: CoordLocalErr}
		}
	} else {
		if localNamespace == nil {
			return ErrNamespaceNotCreated
		}
		localNamespace.Close()
	}
	return nil
}

func (self *DataCoordinator) getRaftAddrForNode(nid string) (string, *CoordErr) {
	node, err := self.register.GetNodeInfo(nid)
	if err != nil {
		return "", &CoordErr{ErrMsg: err.Error(), ErrCode: RpcNoErr, ErrType: CoordRegisterErr}
	}
	return node.RaftTransportAddr, nil
}

func (self *DataCoordinator) prepareNamespaceConf(nsInfo *PartitionMetaInfo) (*node.NamespaceConfig, *CoordErr) {
	raftID, ok := nsInfo.RaftIDs[self.GetMyID()]
	if !ok {
		CoordLog().Warningf("namespace %v has no raft id for local: %v", nsInfo.GetDesp(), nsInfo)
		return nil, ErrNamespaceConfInvalid
	}
	var err *CoordErr
	nsConf := node.NewNSConfig()
	nsConf.BaseName = nsInfo.Name
	nsConf.Name = nsInfo.GetDesp()
	nsConf.EngType = nsInfo.EngType
	nsConf.PartitionNum = nsInfo.PartitionNum
	nsConf.Replicator = nsInfo.Replica
	nsConf.OptimizedFsync = nsInfo.OptimizedFsync
	nsConf.RaftGroupConf.GroupID = uint64(nsInfo.MinGID) + uint64(nsInfo.Partition)
	nsConf.RaftGroupConf.SeedNodes = make([]node.ReplicaInfo, 0)
	for _, nid := range nsInfo.GetISR() {
		var rinfo node.ReplicaInfo
		if nid == self.GetMyID() {
			rinfo.NodeID = self.GetMyRegID()
			rinfo.ReplicaID = raftID
			rinfo.RaftAddr = self.myNode.RaftTransportAddr
		} else {
			rinfo.NodeID = ExtractRegIDFromGenID(nid)
			rid, ok := nsInfo.RaftIDs[nid]
			if !ok {
				CoordLog().Infof("can not found raft id for node: %v, %v", nid, nsInfo.RaftIDs)
				continue
			}
			rinfo.ReplicaID = rid
			rinfo.RaftAddr, err = self.getRaftAddrForNode(nid)
			if err != nil {
				CoordLog().Infof("can not found raft address for node: %v, %v", nid, err)
				continue
			}
		}
		nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, rinfo)
	}
	if len(nsConf.RaftGroupConf.SeedNodes) == 0 {
		CoordLog().Warningf("can not found any seed nodes for namespace: %v", nsInfo)
		return nil, ErrNamespaceConfInvalid
	}
	return nsConf, nil
}

func (self *DataCoordinator) requestJoinNamespaceGroup(raftID uint64, nsInfo *PartitionMetaInfo,
	localNamespace *node.NamespaceNode, remoteNode string) error {
	var m common.MemberInfo
	m.ID = raftID
	m.NodeID = self.GetMyRegID()
	m.GroupID = uint64(nsInfo.MinGID) + uint64(nsInfo.Partition)
	m.GroupName = nsInfo.GetDesp()
	localNamespace.Node.FillMyMemberInfo(&m)
	CoordLog().Infof("request to %v for join member: %v", remoteNode, m)
	if remoteNode == self.GetMyID() {
		return nil
	}
	nip, _, _, httpPort := ExtractNodeInfoFromID(remoteNode)
	d, _ := json.Marshal(m)
	_, err := common.APIRequest("POST",
		"http://"+net.JoinHostPort(nip, httpPort)+common.APIAddNode,
		bytes.NewReader(d), time.Second*3, nil)
	if err != nil {
		CoordLog().Infof("failed to request join namespace: %v", err)
		return err
	}
	return nil
}

func (self *DataCoordinator) tryCheckNamespaces() {
	time.Sleep(time.Second)
	select {
	case self.tryCheckUnsynced <- true:
	default:
	}
}

func (self *DataCoordinator) ensureJoinNamespaceGroup(nsInfo PartitionMetaInfo,
	localNamespace *node.NamespaceNode, firstLoad bool) *CoordErr {

	rm, ok := nsInfo.Removings[self.GetMyID()]
	if ok {
		// for removing node, we just restart local raft node and
		// wait sync from raft leader.
		// For the node not in the raft we will remove this node later so
		// no need request join again.
		if rm.RemoveReplicaID == nsInfo.RaftIDs[self.GetMyID()] {
			CoordLog().Infof("no need request join for removing node: %v, %v", nsInfo.GetDesp(), nsInfo.Removings)
			return nil
		}
	}

	// check if in local raft group
	myRunning := atomic.AddInt32(&self.catchupRunning, 1)
	defer atomic.AddInt32(&self.catchupRunning, -1)
	if myRunning > MAX_RAFT_JOIN_RUNNING {
		CoordLog().Infof("catching too much running: %v", myRunning)
		self.tryCheckNamespaces()
		return ErrCatchupRunningBusy
	}

	dyConf := &node.NamespaceDynamicConf{}
	localNamespace.SetDynamicInfo(*dyConf)
	if localNamespace.IsDataNeedFix() {
		// clean local data
	}
	localNamespace.SetDataFixState(false)
	raftID, ok := nsInfo.RaftIDs[self.GetMyID()]
	if !ok {
		CoordLog().Warningf("namespace %v failed to get raft id %v while check join", nsInfo.GetDesp(),
			nsInfo.RaftIDs)
		return ErrNamespaceConfInvalid
	}
	var joinErr *CoordErr
	retry := 0
	startCheck := time.Now()
	for time.Since(startCheck) < time.Second*30 {
		mems := localNamespace.GetMembers()
		memsMap := make(map[uint64]*common.MemberInfo)
		alreadyJoined := false
		for _, m := range mems {
			memsMap[m.NodeID] = m
			if m.NodeID == self.GetMyRegID() &&
				m.GroupName == nsInfo.GetDesp() &&
				m.ID == raftID {
				if len(mems) > len(nsInfo.GetISR())/2 {
					alreadyJoined = true
				} else {
					CoordLog().Infof("namespace %v is in the small raft group %v, need join large group:%v",
						nsInfo.GetDesp(), mems, nsInfo.RaftNodes)
				}
			}
		}
		if alreadyJoined {
			if localNamespace.IsRaftSynced(firstLoad) {
				joinErr = nil
				break
			}
			CoordLog().Infof("namespace %v still waiting raft synced", nsInfo.GetDesp())
			select {
			case <-self.stopChan:
				return ErrNamespaceExiting
			case <-time.After(time.Second / 2):
			}
			joinErr = ErrNamespaceWaitingSync
		} else {
			joinErr = ErrNamespaceWaitingSync
			var remote string
			cnt := 0
			isr := nsInfo.GetISR()
			if len(isr) == 0 {
				isr = nsInfo.RaftNodes
			}
			for cnt <= len(isr) {
				remote = isr[retry%len(isr)]
				retry++
				cnt++
				if remote == self.GetMyID() {
					continue
				}
				if _, ok := memsMap[ExtractRegIDFromGenID(remote)]; !ok {
					break
				}
			}
			time.Sleep(time.Millisecond * 100)
			if !self.isNamespaceShouldStart(nsInfo) {
				return ErrNamespaceExiting
			}
			self.requestJoinNamespaceGroup(raftID, &nsInfo, localNamespace, remote)
			select {
			case <-self.stopChan:
				return ErrNamespaceExiting
			case <-time.After(time.Second / 2):
			}
		}
	}
	if joinErr != nil {
		self.tryCheckNamespaces()
		CoordLog().Infof("local namespace join failed: %v, retry later: %v", joinErr, nsInfo.GetDesp())
	} else if retry > 0 {
		CoordLog().Infof("local namespace join done: %v", nsInfo.GetDesp())
	}
	return joinErr
}

func (self *DataCoordinator) updateLocalNamespace(nsInfo *PartitionMetaInfo, forceStandaloneCluster bool) (*node.NamespaceNode, *CoordErr) {
	// check namespace exist and prepare on local.
	raftID, ok := nsInfo.RaftIDs[self.GetMyID()]
	if !ok {
		CoordLog().Warningf("namespace %v has no raft id for local", nsInfo.GetDesp(), nsInfo.RaftIDs)
		return nil, ErrNamespaceConfInvalid
	}
	nsConf, err := self.prepareNamespaceConf(nsInfo)
	if err != nil {
		CoordLog().Warningf("prepare join namespace %v failed: %v", nsInfo.GetDesp(), err)
		return nil, err
	}

	// TODO: handle if we need join the existing cluster
	// we should not create new cluster except for the init
	// if something wrong while disaster, we need re-init cluster but that
	// should be used by setting manual flag for re-init
	join := true
	if len(nsInfo.GetISR()) > 0 && nsInfo.GetISR()[0] == self.GetMyID() {
		nid, epoch, err := self.register.GetNamespaceLeader(nsInfo.Name, nsInfo.Partition)
		if err != nil {
			if err != ErrKeyNotFound {
				go self.tryCheckNamespaces()
				return nil, ErrRegisterServiceUnstable
			}
		}
		if nid == "" && epoch == 0 {
			join = false
			CoordLog().Infof("my node will create new cluster for namespace: %v since first init: %v",
				nsInfo.GetDesp(), nsConf)
		}
	}
	if forceStandaloneCluster {
		join = false
	}
	localNode, _ := self.localNSMgr.InitNamespaceNode(nsConf, raftID, join)
	if localNode != nil {
		if checkErr := localNode.CheckRaftConf(raftID, nsConf); checkErr != nil {
			CoordLog().Infof("local namespace %v mismatch with the new raft config removing: %v", nsInfo.GetDesp(), checkErr)
			return nil, &CoordErr{ErrMsg: checkErr.Error(), ErrCode: RpcNoErr, ErrType: CoordLocalErr}
		}
	}
	if localNode == nil {
		CoordLog().Warningf("local namespace %v init failed", nsInfo.GetDesp())
		return nil, ErrLocalInitNamespaceFailed
	}

	localErr := localNode.SetMagicCode(nsInfo.MagicCode)
	if localErr != nil {
		CoordLog().Warningf("local namespace %v init magic code failed: %v", nsInfo.GetDesp(), localErr)
		return localNode, ErrLocalInitNamespaceFailed
	}
	dyConf := &node.NamespaceDynamicConf{}
	localNode.SetDynamicInfo(*dyConf)
	if err := localNode.Start(forceStandaloneCluster); err != nil {
		return nil, ErrLocalInitNamespaceFailed
	}
	return localNode, nil
}

func (self *DataCoordinator) RestartAsStandalone(fullNamespace string) error {
	namespace, pid := common.GetNamespaceAndPartition(fullNamespace)
	if namespace == "" {
		CoordLog().Warningf("namespace invalid: %v", fullNamespace)
		return ErrNamespaceInvalid
	}
	nsInfo, err := self.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		return err
	}

	localNs := self.localNSMgr.GetNamespaceNode(fullNamespace)
	if localNs == nil {
		return ErrNamespaceNotFound
	}
	CoordLog().Warningf("namespace %v restart as standalone cluster", fullNamespace)
	localNs.Close()
	_, coordErr := self.updateLocalNamespace(nsInfo, true)
	if coordErr != nil {
		return coordErr.ToErrorType()
	}
	return nil
}

func (self *DataCoordinator) GetSnapshotSyncInfo(fullNamespace string) ([]common.SnapshotSyncInfo, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNamespace)
	if namespace == "" {
		CoordLog().Warningf("namespace invalid: %v", fullNamespace)
		return nil, ErrNamespaceInvalid
	}
	nsInfo, err := self.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		return nil, err
	}
	var ssiList []common.SnapshotSyncInfo
	for _, nid := range nsInfo.GetISR() {
		node, err := self.register.GetNodeInfo(nid)
		if err != nil {
			continue
		}
		var ssi common.SnapshotSyncInfo
		ssi.NodeID = node.RegID
		ssi.DataRoot = node.DataRoot
		ssi.ReplicaID = nsInfo.RaftIDs[nid]
		ssi.RemoteAddr = node.NodeIP
		ssi.HttpAPIPort = node.HttpPort
		ssi.RsyncModule = node.RsyncModule
		ssiList = append(ssiList, ssi)
	}
	return ssiList, nil
}

func (self *DataCoordinator) IsRemovingMember(m common.MemberInfo) (bool, error) {
	namespace, pid := common.GetNamespaceAndPartition(m.GroupName)
	if namespace == "" {
		CoordLog().Warningf("namespace invalid: %v", m.GroupName)
		return false, ErrNamespaceInvalid
	}
	nsInfo, err := self.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		if err == ErrKeyNotFound {
			return true, nil
		}
		return false, err
	}

	for nid, rm := range nsInfo.Removings {
		if rm.RemoveReplicaID == m.ID && ExtractRegIDFromGenID(nid) == m.NodeID {
			return true, nil
		}
	}
	return false, nil
}

func (self *DataCoordinator) GetNamespaceLeader(fullNS string) (uint64, int64, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNS)
	if namespace == "" {
		CoordLog().Warningf("namespace invalid: %v", fullNS)
		return 0, 0, ErrNamespaceInvalid
	}
	nid, epoch, err := self.register.GetNamespaceLeader(namespace, pid)
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if nid == "" {
		return 0, int64(epoch), nil
	}
	regID := ExtractRegIDFromGenID(nid)
	return regID, int64(epoch), nil
}

func (self *DataCoordinator) UpdateMeForNamespaceLeader(fullNS string, oldEpoch int64) (int64, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNS)
	if namespace == "" {
		CoordLog().Warningf("namespace invalid: %v", fullNS)
		return 0, ErrNamespaceInvalid
	}
	var rl RealLeader
	rl.Leader = self.GetMyID()
	epoch, err := self.register.UpdateNamespaceLeader(namespace, pid, rl, EpochType(oldEpoch))
	return int64(epoch), err
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (self *DataCoordinator) prepareLeavingCluster() {
	CoordLog().Infof("I am prepare leaving the cluster.")
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
			// only leader check the follower status
			leader := self.getNamespaceRaftLeader(nsInfo.GetCopy())
			if leader != self.GetMyRegID() {
				continue
			}
			for _, newLeader := range nsInfo.GetISR() {
				if newLeader == self.GetMyID() {
					continue
				}
				self.transferMyNamespaceLeader(nsInfo.GetCopy(), newLeader)
				break
			}
		}
	}
	if self.register != nil {
		atomic.StoreInt32(&self.stopping, 1)
		self.register.Unregister(&self.myNode)
		self.register.Stop()
	}

	CoordLog().Infof("prepare leaving finished.")
	self.localNSMgr.Stop()
}

func (self *DataCoordinator) Stats(namespace string, part int) *CoordStats {
	s := &CoordStats{}
	s.NsCoordStats = make([]NamespaceCoordStat, 0)
	if len(namespace) > 0 {
		meta, err := self.register.GetNamespaceMetaInfo(namespace)
		if err != nil {
			CoordLog().Infof("failed to get namespace info: %v", err)
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

package datanode_coord

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	node "github.com/absolute8511/ZanRedisDB/node"
)

var (
	MaxRetryWait         = time.Second * 3
	ErrNamespaceNotReady = cluster.NewCoordErr("namespace node is not ready", cluster.CoordLocalErr)
	ErrNamespaceInvalid  = errors.New("namespace name is invalid")
	ErrNamespaceNotFound = errors.New("namespace is not found")
	TransferLeaderWait   = time.Second * 10
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

type PartitionList []cluster.PartitionMetaInfo

func (pl PartitionList) Len() int { return len(pl) }
func (pl PartitionList) Less(i, j int) bool {
	return pl[i].Partition < pl[j].Partition
}
func (pl PartitionList) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

type DataCoordinator struct {
	clusterKey       string
	register         cluster.DataNodeRegister
	pdMutex          sync.Mutex
	pdLeader         cluster.NodeInfo
	myNode           cluster.NodeInfo
	stopChan         chan struct{}
	tryCheckUnsynced chan bool
	wg               sync.WaitGroup
	stopping         int32
	catchupRunning   int32
	localNSMgr       *node.NamespaceMgr
}

func NewDataCoordinator(cluster string, nodeInfo *cluster.NodeInfo, nsMgr *node.NamespaceMgr) *DataCoordinator {
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

func (dc *DataCoordinator) GetMyID() string {
	return dc.myNode.GetID()
}

func (dc *DataCoordinator) GetMyRegID() uint64 {
	return dc.myNode.RegID
}

func (dc *DataCoordinator) SetRegister(l cluster.DataNodeRegister) error {
	dc.register = l
	if dc.register != nil {
		dc.register.InitClusterID(dc.clusterKey)
		if dc.myNode.RegID <= 0 {
			var err error
			dc.myNode.RegID, err = dc.register.NewRegisterNodeID()
			if err != nil {
				cluster.CoordLog().Errorf("failed to init node register id: %v", err)
				return err
			}
			err = dc.localNSMgr.SaveMachineRegID(dc.myNode.RegID)
			if err != nil {
				cluster.CoordLog().Errorf("failed to save register id: %v", err)
				return err
			}
		}
		dc.myNode.ID = cluster.GenNodeID(&dc.myNode, "datanode")
		cluster.CoordLog().Infof("node start with register id: %v", dc.myNode.RegID)
	}
	return nil
}

func (dc *DataCoordinator) Start() error {
	if dc.register != nil {
		if dc.myNode.RegID <= 0 {
			cluster.CoordLog().Errorf("invalid register id: %v", dc.myNode.RegID)
			return errors.New("invalid register id for data node")
		}
		err := dc.register.Register(&dc.myNode)
		if err != nil {
			cluster.CoordLog().Warningf("failed to register coordinator: %v", err)
			return err
		}
	}
	if dc.localNSMgr != nil {
		dc.localNSMgr.Start()
	}
	dc.wg.Add(1)
	go dc.watchPD()

	err := dc.loadLocalNamespaceData()
	if err != nil {
		close(dc.stopChan)
		return err
	}

	dc.wg.Add(1)
	go dc.checkForUnsyncedNamespaces()
	return nil
}

func (dc *DataCoordinator) Stop() {
	if !atomic.CompareAndSwapInt32(&dc.stopping, 0, 1) {
		return
	}
	dc.prepareLeavingCluster()
	close(dc.stopChan)
	dc.wg.Wait()
}

func (dc *DataCoordinator) GetCurrentPD() cluster.NodeInfo {
	dc.pdMutex.Lock()
	defer dc.pdMutex.Unlock()
	return dc.pdLeader
}

func (dc *DataCoordinator) GetAllPDNodes() ([]cluster.NodeInfo, error) {
	return dc.register.GetAllPDNodes()
}

func (dc *DataCoordinator) watchPD() {
	defer dc.wg.Done()
	leaderChan := make(chan *cluster.NodeInfo, 1)
	if dc.register != nil {
		go dc.register.WatchPDLeader(leaderChan, dc.stopChan)
	} else {
		return
	}
	for {
		select {
		case n, ok := <-leaderChan:
			if !ok {
				return
			}
			dc.pdMutex.Lock()
			if n.GetID() != dc.pdLeader.GetID() {
				cluster.CoordLog().Infof("pd leader changed from %v to %v", dc.pdLeader, n)
				dc.pdLeader = *n
			}
			dc.pdMutex.Unlock()
		}
	}
}

func (dc *DataCoordinator) checkLocalNamespaceMagicCode(nsInfo *cluster.PartitionMetaInfo, tryFix bool) error {
	if nsInfo.MagicCode <= 0 {
		return nil
	}
	err := dc.localNSMgr.CheckMagicCode(nsInfo.GetDesp(), nsInfo.MagicCode, tryFix)
	if err != nil {
		cluster.CoordLog().Infof("namespace %v check magic code error: %v", nsInfo.GetDesp(), err)
		return err
	}
	return nil
}

func (dc *DataCoordinator) loadLocalNamespaceData() error {
	if dc.localNSMgr == nil {
		return nil
	}
	namespaceMap, _, err := dc.register.GetAllNamespaces()
	if err != nil {
		if err == cluster.ErrKeyNotFound {
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
			localNamespace := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			shouldLoad := dc.isNamespaceShouldStart(nsInfo)
			if !shouldLoad {
				if len(nsInfo.GetISR()) >= nsInfo.Replica && localNamespace != nil {
					dc.removeLocalNamespaceFromRaft(localNamespace, false)
				}
				continue
			}
			if localNamespace != nil {
				// already loaded
				joinErr := dc.ensureJoinNamespaceGroup(nsInfo, localNamespace, false)
				if joinErr != nil && joinErr != cluster.ErrNamespaceConfInvalid {
					// we ensure join group as order for partitions
					break
				}
				continue
			}
			cluster.CoordLog().Infof("loading namespace: %v", nsInfo.GetDesp())
			if namespaceName == "" {
				continue
			}
			checkErr := dc.checkLocalNamespaceMagicCode(&nsInfo, true)
			if checkErr != nil {
				cluster.CoordLog().Errorf("failed to check namespace :%v, err:%v", nsInfo.GetDesp(), checkErr)
				continue
			}

			localNamespace, coordErr := dc.updateLocalNamespace(&nsInfo, false)
			if coordErr != nil {
				cluster.CoordLog().Errorf("failed to init/update local namespace %v: %v", nsInfo.GetDesp(), coordErr)
				continue
			}

			dyConf := &node.NamespaceDynamicConf{}
			localNamespace.SetDynamicInfo(*dyConf)
			localErr := dc.checkAndFixLocalNamespaceData(&nsInfo, localNamespace)
			if localErr != nil {
				cluster.CoordLog().Errorf("check local namespace %v data need to be fixed:%v", nsInfo.GetDesp(), localErr)
				localNamespace.SetDataFixState(true)
			}
			joinErr := dc.ensureJoinNamespaceGroup(nsInfo, localNamespace, true)
			if joinErr != nil && joinErr != cluster.ErrNamespaceConfInvalid {
				// we ensure join group as order for partitions
				break
			}
		}
	}
	return nil
}

func (dc *DataCoordinator) isMeInRaftGroup(nsInfo *cluster.PartitionMetaInfo) (bool, error) {
	var lastErr error
	for _, remoteNode := range nsInfo.GetISR() {
		if remoteNode == dc.GetMyID() {
			continue
		}
		nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
		var rsp []*common.MemberInfo
		code, err := common.APIRequest("GET",
			"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetMembers+"/"+nsInfo.GetDesp(),
			nil, time.Second*3, &rsp)
		if err != nil {
			cluster.CoordLog().Infof("failed to get members from %v for namespace: %v, %v", nip, nsInfo.GetDesp(), err)
			if code == 404 {
				lastErr = ErrNamespaceNotFound
			} else {
				lastErr = err
			}
			continue
		}

		for _, m := range rsp {
			if m.NodeID == dc.GetMyRegID() && m.ID == nsInfo.RaftIDs[dc.GetMyID()] {
				cluster.CoordLog().Infof("from %v for namespace: %v, node is still in raft: %v", nip, nsInfo.GetDesp(), m)
				return true, nil
			}
		}
	}
	return false, lastErr
}

func (dc *DataCoordinator) isNamespaceShouldStart(nsInfo cluster.PartitionMetaInfo) bool {
	// it may happen that the node marked as removing can not be removed because
	// the raft group has not enough quorum to do the proposal to change the configure.
	// In this way we need start the removing node to join the raft group and then
	// the leader can remove the node from the raft group, and then we can safely remove
	// the removing node finally
	shouldLoad := cluster.FindSlice(nsInfo.RaftNodes, dc.GetMyID()) != -1
	if !shouldLoad {
		return false
	}
	rm, ok := nsInfo.Removings[dc.GetMyID()]
	if !ok {
		return true
	}
	if rm.RemoveReplicaID != nsInfo.RaftIDs[dc.GetMyID()] {
		return true
	}

	inRaft, err := dc.isMeInRaftGroup(&nsInfo)
	if inRaft || err == ErrNamespaceNotFound {
		cluster.CoordLog().Infof("removing node %v-%v should join namespace %v since still in raft",
			dc.GetMyID(), rm.RemoveReplicaID, nsInfo.GetDesp())
		return true
	}
	return false
}

func (dc *DataCoordinator) isNamespaceShouldStop(nsInfo cluster.PartitionMetaInfo, localNamespace *node.NamespaceNode) bool {
	// removing node can stop local raft only when all the other members
	// are notified to remove this node
	// Mostly, the remove node proposal will handle the raft node stop, however
	// there are some situations to be checked here.
	inMeta := false
	for _, nid := range nsInfo.RaftNodes {
		if nid == dc.GetMyID() {
			replicaID := nsInfo.RaftIDs[nid]
			if replicaID == localNamespace.GetRaftID() {
				inMeta = true
				break
			}
		}
	}
	var rmID uint64
	if inMeta {
		rm, ok := nsInfo.Removings[dc.GetMyID()]
		if !ok {
			return false
		}
		rmID = rm.RemoveReplicaID
		if rm.RemoveReplicaID != nsInfo.RaftIDs[dc.GetMyID()] {
			return false
		}
	} else {
		cluster.CoordLog().Infof("no any meta info for this namespace: %v", nsInfo.GetDesp(), localNamespace.GetRaftID())
	}

	inRaft, err := dc.isMeInRaftGroup(&nsInfo)
	if inRaft || err != nil {
		return false
	}
	mems := localNamespace.GetMembers()
	for _, m := range mems {
		if m.ID == rmID {
			return false
		}
	}
	cluster.CoordLog().Infof("removing node %v-%v should stop namespace %v (replica %v) since not in any raft group anymore",
		dc.GetMyID(), rmID, nsInfo.GetDesp(), localNamespace.GetRaftID())
	return true
}

func (dc *DataCoordinator) checkAndFixLocalNamespaceData(nsInfo *cluster.PartitionMetaInfo, localNamespace *node.NamespaceNode) error {
	return nil
}

func (dc *DataCoordinator) addNamespaceRaftMember(nsInfo *cluster.PartitionMetaInfo, m *common.MemberInfo) {
	for nid, removing := range nsInfo.Removings {
		if m.ID == removing.RemoveReplicaID && m.NodeID == cluster.ExtractRegIDFromGenID(nid) {
			cluster.CoordLog().Infof("raft member %v is marked as removing in meta: %v, ignore add raft member", m, nsInfo.Removings)
			return
		}
	}
	nsNode := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		cluster.CoordLog().Infof("namespace %v not found while add member", nsInfo.GetDesp())
		return
	}
	err := nsNode.Node.ProposeAddMember(*m)
	if err != nil {
		cluster.CoordLog().Infof("%v propose add %v failed: %v", nsInfo.GetDesp(), m, err)
	} else {
		cluster.CoordLog().Infof("namespace %v propose add member %v", nsInfo.GetDesp(), m)
	}
}

func (dc *DataCoordinator) removeNamespaceRaftMember(nsInfo *cluster.PartitionMetaInfo, m *common.MemberInfo) {
	nsNode := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		cluster.CoordLog().Infof("namespace %v not found while remove member", nsInfo.GetDesp())
		return
	}

	err := nsNode.Node.ProposeRemoveMember(*m)
	if err != nil {
		cluster.CoordLog().Infof("propose remove %v failed: %v", m, err)
	} else {
		cluster.CoordLog().Infof("namespace %v propose remove member %v", nsInfo.GetDesp(), m)
	}
}

func (dc *DataCoordinator) getNamespaceRaftMembers(nsInfo *cluster.PartitionMetaInfo) []*common.MemberInfo {
	nsNode := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return nil
	}
	return nsNode.Node.GetMembers()
}

func (dc *DataCoordinator) getNamespaceRaftLeader(nsInfo *cluster.PartitionMetaInfo) uint64 {
	nsNode := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return 0
	}
	m := nsNode.Node.GetLeadMember()
	if m == nil {
		return 0
	}
	return m.NodeID
}

func (dc *DataCoordinator) transferMyNamespaceLeader(nsInfo *cluster.PartitionMetaInfo, nid string, force bool) bool {
	nsNode := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
	if nsNode == nil {
		return false
	}
	toRaftID, ok := nsInfo.RaftIDs[nid]
	if !ok {
		cluster.CoordLog().Warningf("transfer namespace %v leader to %v failed for missing raft id: %v",
			nsInfo.GetDesp(), nid, nsInfo.RaftIDs)
		return false
	}

	if !force {
		if time.Now().UnixNano()-nsNode.GetLastLeaderChangedTime() < time.Minute.Nanoseconds() {
			return false
		}
		if !dc.isReplicaReadyForRaft(nsNode, toRaftID) {
			return false
		}
	}
	cluster.CoordLog().Infof("begin transfer namespace %v leader to %v", nsInfo.GetDesp(), nid)
	err := nsNode.TransferMyLeader(cluster.ExtractRegIDFromGenID(nid), toRaftID)
	if err != nil {
		cluster.CoordLog().Infof("failed to transfer namespace %v leader to %v: %v", nsInfo.GetDesp(), nid, err)
		return false
	}
	return true
}

// check on leader if the replica ready for raft, which means this replica is most updated
// and have the nearly the newest raft logs.
func (dc *DataCoordinator) isReplicaReadyForRaft(nsNode *node.NamespaceNode, toRaftID uint64) bool {
	if nsNode.IsReady() && nsNode.Node.IsReplicaRaftReady(toRaftID) {
		return true
	}
	if nsNode.IsReady() {
		stats := nsNode.Node.GetRaftStatus()
		cluster.CoordLog().Infof("namespace %v raft status still not ready for node:%v, %v",
			nsNode.FullName(), toRaftID, stats)
	}
	return false
}

func (dc *DataCoordinator) checkForUnsyncedNamespaces() {
	ticker := time.NewTicker(time.Minute * 5)
	defer dc.wg.Done()
	type pendingRemoveInfo struct {
		ts time.Time
		m  common.MemberInfo
	}
	pendingRemovings := make(map[string]map[uint64]pendingRemoveInfo)
	// avoid transfer too much partitions in the same time
	lastTransferredTime := time.Now()
	doWork := func() {
		if atomic.LoadInt32(&dc.stopping) == 1 {
			return
		}
		cluster.CoordLog().Debugf("check for namespace sync...")
		// try load local namespace if any namespace raft group changed
		dc.loadLocalNamespaceData()

		// check local namespaces with cluster to remove the unneed data
		tmpChecks := dc.localNSMgr.GetNamespaces()
		allIndexSchemas := make(map[string]map[string]*common.IndexSchema)
		allNamespaces, _, err := dc.register.GetAllNamespaces()
		if err != nil {
			return
		}
		for ns, parts := range allNamespaces {
			tableSchemas := make(map[string]*common.IndexSchema)
			schemas, err := dc.register.GetNamespaceSchemas(ns)
			if err != nil {
				if err != cluster.ErrKeyNotFound {
					cluster.CoordLog().Infof("get schema info failed: %v", err)
				}
				continue
			}
			if len(parts) == 0 || len(parts) != parts[0].PartitionNum {
				continue
			}
			for table, schemaData := range schemas {
				var indexes common.IndexSchema
				err := json.Unmarshal(schemaData.Schema, &indexes)
				if err != nil {
					cluster.CoordLog().Infof("unmarshal schema data failed: %v", err)
					continue
				}
				tableSchemas[table] = &indexes
			}
			allIndexSchemas[ns] = tableSchemas
		}
		for name, localNamespace := range tmpChecks {
			namespace, pid := common.GetNamespaceAndPartition(name)
			if namespace == "" {
				cluster.CoordLog().Warningf("namespace invalid: %v", name)
				continue
			}
			namespaceMeta, err := dc.register.GetNamespacePartInfo(namespace, pid)
			if err != nil {
				if err == cluster.ErrKeyNotFound {
					cluster.CoordLog().Infof("the namespace should be clean since not found in register: %v", name)
					_, err = dc.register.GetNamespaceMetaInfo(namespace)
					if err == cluster.ErrKeyNotFound {
						dc.forceRemoveLocalNamespace(localNamespace)
					}
				}
				cluster.CoordLog().Infof("got namespace %v meta failed: %v", name, err)
				go dc.tryCheckNamespaces()
				continue
			}
			if dc.isNamespaceShouldStop(*namespaceMeta, localNamespace) {
				dc.forceRemoveLocalNamespace(localNamespace)
				continue
			}
			leader := dc.getNamespaceRaftLeader(namespaceMeta)
			if leader == 0 {
				continue
			}
			isrList := namespaceMeta.GetISR()
			localRID := localNamespace.GetRaftID()

			if localRID != namespaceMeta.RaftIDs[dc.myNode.GetID()] {
				if len(isrList) > 0 {
					cluster.CoordLog().Infof("the namespace should be clean : %v", namespaceMeta)
					dc.removeLocalNamespaceFromRaft(localNamespace, true)
				}
				continue
			}
			// only leader check the follower status
			if leader != dc.GetMyRegID() || len(isrList) == 0 {
				continue
			}
			isReplicasEnough := len(isrList) >= namespaceMeta.Replica
			members := dc.getNamespaceRaftMembers(namespaceMeta)
			if len(members) < namespaceMeta.Replica {
				isReplicasEnough = false
			}

			if cluster.FindSlice(isrList, dc.GetMyID()) == -1 {
				cluster.CoordLog().Infof("namespace %v leader is not in isr: %v, maybe removing",
					namespaceMeta.GetDesp(), isrList)
				done := false
				if time.Since(lastTransferredTime) >= TransferLeaderWait {
					done = dc.transferMyNamespaceLeader(namespaceMeta, isrList[0], false)
				}
				if !done {
					go dc.tryCheckNamespacesIn(TransferLeaderWait)
				} else {
					lastTransferredTime = time.Now()
				}
				continue
			}
			if isReplicasEnough && isrList[0] != dc.GetMyID() {
				// the raft leader check if I am the expected sharding leader,
				// if not, try to transfer the leader to expected node. We need do this
				// because we should make all the sharding leaders balanced on
				// all the cluster nodes.
				// avoid transfer while some node is leaving, so wait enough time to
				// allow node leaving
				// also we should avoid transfer leader while some node is catchuping while recover from restart
				done := false
				if time.Since(lastTransferredTime) >= TransferLeaderWait {
					done = dc.transferMyNamespaceLeader(namespaceMeta, isrList[0], false)
				}
				if !done {
					go dc.tryCheckNamespacesIn(TransferLeaderWait)
				} else {
					lastTransferredTime = time.Now()
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
							if m.NodeID != cluster.ExtractRegIDFromGenID(nid) {
								cluster.CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
							}
							break
						}
					}
					if !found {
						anyWaitingJoin = true
						cluster.CoordLog().Infof("namespace %v new node still waiting join raft : %v, %v", namespaceMeta.GetDesp(), rid, nid)
					}
				}
				if anyWaitingJoin || len(members) < len(isrList) {
					go dc.tryCheckNamespaces()
					continue
				}
				isFullStable := true
				// the members is more than replica, we need to remove the member that is not necessary anymore
				pendings, ok := pendingRemovings[namespaceMeta.GetDesp()]
				if !ok {
					pendings = make(map[uint64]pendingRemoveInfo)
					pendingRemovings[namespaceMeta.GetDesp()] = pendings
					isFullStable = false
				}
				newestReplicaInfo, err := dc.register.GetRemoteNamespaceReplicaInfo(namespaceMeta.Name, namespaceMeta.Partition)
				if err != nil {
					if err != cluster.ErrKeyNotFound {
						go dc.tryCheckNamespaces()
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
							if m.NodeID != cluster.ExtractRegIDFromGenID(nid) {
								cluster.CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
							}
							break
						}
					}
					if !found {
						isFullStable = false
						cluster.CoordLog().Infof("raft member %v not found in meta: %v", m, newestReplicaInfo.RaftNodes)
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
								cluster.CoordLog().Infof("pending removing member %v finally removed since not in meta", pendRemove)
								dc.removeNamespaceRaftMember(namespaceMeta, m)
							}
						} else {
							pendings[m.ID] = pendingRemoveInfo{
								ts: time.Now(),
								m:  *m,
							}
						}
						go dc.tryCheckNamespaces()
					} else {
						delete(pendings, m.ID)
						for nid, removing := range newestReplicaInfo.Removings {
							isFullStable = false
							if m.ID == removing.RemoveReplicaID && m.NodeID == cluster.ExtractRegIDFromGenID(nid) {
								cluster.CoordLog().Infof("raft member %v is marked as removing in meta: %v", m, newestReplicaInfo.Removings)
								dc.removeNamespaceRaftMember(namespaceMeta, m)
							}
						}
					}
				}
				if isFullStable {
					indexSchemas, ok := allIndexSchemas[namespace]
					if ok {
						dc.doSyncSchemaInfo(localNamespace, indexSchemas)
					}
				}

			}
		}
	}

	nsChangedChan := dc.register.GetNamespacesNotifyChan()
	for {
		select {
		case <-dc.stopChan:
			return
		case <-dc.tryCheckUnsynced:
			doWork()
		case <-ticker.C:
			doWork()
		case <-nsChangedChan:
			cluster.CoordLog().Infof("trigger check by namespace changed")
			doWork()
		}
	}
}

func (dc *DataCoordinator) forceRemoveLocalNamespace(localNamespace *node.NamespaceNode) {
	err := localNamespace.Destroy()
	if err != nil {
		cluster.CoordLog().Infof("failed to force remove local data: %v", err)
	}
}

func (dc *DataCoordinator) removeLocalNamespaceFromRaft(localNamespace *node.NamespaceNode, removeFromRaft bool) *cluster.CoordErr {
	if removeFromRaft {
		if !localNamespace.IsReady() {
			return ErrNamespaceNotReady
		}
		m := localNamespace.Node.GetLocalMemberInfo()
		cluster.CoordLog().Infof("propose remove %v from namespace : %v", m.ID, m.GroupName)

		localErr := localNamespace.Node.ProposeRemoveMember(*m)
		if localErr != nil {
			cluster.CoordLog().Infof("propose remove dc %v failed : %v", m, localErr)
			return &cluster.CoordErr{ErrMsg: localErr.Error(), ErrCode: cluster.RpcCommonErr, ErrType: cluster.CoordLocalErr}
		}
	} else {
		if localNamespace == nil {
			return cluster.ErrNamespaceNotCreated
		}
		localNamespace.Close()
	}
	return nil
}

func (dc *DataCoordinator) getRaftAddrForNode(nid string) (string, *cluster.CoordErr) {
	node, err := dc.register.GetNodeInfo(nid)
	if err != nil {
		return "", &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	}
	return node.RaftTransportAddr, nil
}

func (dc *DataCoordinator) prepareNamespaceConf(nsInfo *cluster.PartitionMetaInfo) (*node.NamespaceConfig, *cluster.CoordErr) {
	raftID, ok := nsInfo.RaftIDs[dc.GetMyID()]
	if !ok {
		cluster.CoordLog().Warningf("namespace %v has no raft id for local: %v", nsInfo.GetDesp(), nsInfo)
		return nil, cluster.ErrNamespaceConfInvalid
	}
	var err *cluster.CoordErr
	nsConf := node.NewNSConfig()
	nsConf.BaseName = nsInfo.Name
	nsConf.Name = nsInfo.GetDesp()
	nsConf.EngType = nsInfo.EngType
	nsConf.PartitionNum = nsInfo.PartitionNum
	nsConf.Replicator = nsInfo.Replica
	nsConf.OptimizedFsync = nsInfo.OptimizedFsync
	if nsInfo.ExpirationPolicy != "" {
		nsConf.ExpirationPolicy = nsInfo.ExpirationPolicy
	}
	if nsInfo.SnapCount > 100 {
		nsConf.SnapCount = nsInfo.SnapCount
		nsConf.SnapCatchup = nsInfo.SnapCount / 4
	}
	nsConf.RaftGroupConf.GroupID = uint64(nsInfo.MinGID) + uint64(nsInfo.Partition)
	nsConf.RaftGroupConf.SeedNodes = make([]node.ReplicaInfo, 0)
	for _, nid := range nsInfo.GetISR() {
		var rinfo node.ReplicaInfo
		if nid == dc.GetMyID() {
			rinfo.NodeID = dc.GetMyRegID()
			rinfo.ReplicaID = raftID
			rinfo.RaftAddr = dc.myNode.RaftTransportAddr
		} else {
			rinfo.NodeID = cluster.ExtractRegIDFromGenID(nid)
			rid, ok := nsInfo.RaftIDs[nid]
			if !ok {
				cluster.CoordLog().Infof("can not found raft id for node: %v, %v", nid, nsInfo.RaftIDs)
				continue
			}
			rinfo.ReplicaID = rid
			rinfo.RaftAddr, err = dc.getRaftAddrForNode(nid)
			if err != nil {
				cluster.CoordLog().Infof("can not found raft address for node: %v, %v", nid, err)
				continue
			}
		}
		nsConf.RaftGroupConf.SeedNodes = append(nsConf.RaftGroupConf.SeedNodes, rinfo)
	}
	if len(nsConf.RaftGroupConf.SeedNodes) == 0 {
		cluster.CoordLog().Warningf("can not found any seed nodes for namespace: %v", nsInfo)
		return nil, cluster.ErrNamespaceConfInvalid
	}
	return nsConf, nil
}

func (dc *DataCoordinator) requestJoinNamespaceGroup(raftID uint64, nsInfo *cluster.PartitionMetaInfo,
	localNamespace *node.NamespaceNode, remoteNode string) error {
	var m common.MemberInfo
	m.ID = raftID
	m.NodeID = dc.GetMyRegID()
	m.GroupID = uint64(nsInfo.MinGID) + uint64(nsInfo.Partition)
	m.GroupName = nsInfo.GetDesp()
	localNamespace.Node.FillMyMemberInfo(&m)
	cluster.CoordLog().Infof("request to %v for join member: %v", remoteNode, m)
	if remoteNode == dc.GetMyID() {
		return nil
	}
	nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
	d, _ := json.Marshal(m)
	_, err := common.APIRequest("POST",
		"http://"+net.JoinHostPort(nip, httpPort)+common.APIAddNode,
		bytes.NewReader(d), time.Second*3, nil)
	if err != nil {
		cluster.CoordLog().Infof("failed to request join namespace: %v", err)
		return err
	}
	return nil
}

func (dc *DataCoordinator) tryCheckNamespacesIn(wait time.Duration) {
	time.Sleep(wait)
	dc.tryCheckNamespaces()
}

func (dc *DataCoordinator) tryCheckNamespaces() {
	time.Sleep(time.Second)
	select {
	case dc.tryCheckUnsynced <- true:
	default:
	}
}

func (dc *DataCoordinator) ensureJoinNamespaceGroup(nsInfo cluster.PartitionMetaInfo,
	localNamespace *node.NamespaceNode, firstLoad bool) *cluster.CoordErr {

	rm, ok := nsInfo.Removings[dc.GetMyID()]
	if ok {
		// for removing node, we just restart local raft node and
		// wait sync from raft leader.
		// For the node not in the raft we will remove this node later so
		// no need request join again.
		if rm.RemoveReplicaID == nsInfo.RaftIDs[dc.GetMyID()] {
			cluster.CoordLog().Infof("no need request join for removing node: %v, %v", nsInfo.GetDesp(), nsInfo.Removings)
			return nil
		}
	}

	// check if in local raft group
	myRunning := atomic.AddInt32(&dc.catchupRunning, 1)
	defer atomic.AddInt32(&dc.catchupRunning, -1)
	if myRunning > MAX_RAFT_JOIN_RUNNING {
		cluster.CoordLog().Infof("catching too much running: %v", myRunning)
		dc.tryCheckNamespaces()
		return cluster.ErrCatchupRunningBusy
	}

	dyConf := &node.NamespaceDynamicConf{}
	localNamespace.SetDynamicInfo(*dyConf)
	if localNamespace.IsDataNeedFix() {
		// clean local data
	}
	localNamespace.SetDataFixState(false)
	raftID, ok := nsInfo.RaftIDs[dc.GetMyID()]
	if !ok {
		cluster.CoordLog().Warningf("namespace %v failed to get raft id %v while check join", nsInfo.GetDesp(),
			nsInfo.RaftIDs)
		return cluster.ErrNamespaceConfInvalid
	}
	var joinErr *cluster.CoordErr
	retry := 0
	startCheck := time.Now()
	for time.Since(startCheck) < time.Second*30 {
		mems := localNamespace.GetMembers()
		memsMap := make(map[uint64]*common.MemberInfo)
		alreadyJoined := false
		for _, m := range mems {
			memsMap[m.NodeID] = m
			if m.NodeID == dc.GetMyRegID() &&
				m.GroupName == nsInfo.GetDesp() &&
				m.ID == raftID {
				if len(mems) > len(nsInfo.GetISR())/2 {
					alreadyJoined = true
				} else {
					cluster.CoordLog().Infof("namespace %v is in the small raft group %v, need join large group:%v",
						nsInfo.GetDesp(), mems, nsInfo.RaftNodes)
				}
			}
		}
		if alreadyJoined {
			if localNamespace.IsRaftSynced(firstLoad) {
				joinErr = nil
				break
			}
			cluster.CoordLog().Infof("namespace %v still waiting raft synced", nsInfo.GetDesp())
			select {
			case <-dc.stopChan:
				return cluster.ErrNamespaceExiting
			case <-time.After(time.Second / 2):
			}
			joinErr = cluster.ErrNamespaceWaitingSync
		} else {
			joinErr = cluster.ErrNamespaceWaitingSync
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
				if remote == dc.GetMyID() {
					continue
				}
				if _, ok := memsMap[cluster.ExtractRegIDFromGenID(remote)]; !ok {
					break
				}
			}
			time.Sleep(time.Millisecond * 100)
			if !dc.isNamespaceShouldStart(nsInfo) {
				return cluster.ErrNamespaceExiting
			}
			dc.requestJoinNamespaceGroup(raftID, &nsInfo, localNamespace, remote)
			select {
			case <-dc.stopChan:
				return cluster.ErrNamespaceExiting
			case <-time.After(time.Second / 2):
			}
		}
	}
	if joinErr != nil {
		dc.tryCheckNamespaces()
		cluster.CoordLog().Infof("local namespace join failed: %v, retry later: %v", joinErr, nsInfo.GetDesp())
	} else if retry > 0 {
		cluster.CoordLog().Infof("local namespace join done: %v", nsInfo.GetDesp())
	}
	return joinErr
}

func (dc *DataCoordinator) updateLocalNamespace(nsInfo *cluster.PartitionMetaInfo, forceStandaloneCluster bool) (*node.NamespaceNode, *cluster.CoordErr) {
	// check namespace exist and prepare on local.
	raftID, ok := nsInfo.RaftIDs[dc.GetMyID()]
	if !ok {
		cluster.CoordLog().Warningf("namespace %v has no raft id for local", nsInfo.GetDesp(), nsInfo.RaftIDs)
		return nil, cluster.ErrNamespaceConfInvalid
	}
	nsConf, err := dc.prepareNamespaceConf(nsInfo)
	if err != nil {
		cluster.CoordLog().Warningf("prepare join namespace %v failed: %v", nsInfo.GetDesp(), err)
		return nil, err
	}

	// TODO: handle if we need join the existing cluster
	// we should not create new cluster except for the init
	// if something wrong while disaster, we need re-init cluster but that
	// should be used by setting manual flag for re-init
	join := true
	if len(nsInfo.GetISR()) > 0 && nsInfo.GetISR()[0] == dc.GetMyID() {
		nid, epoch, err := dc.register.GetNamespaceLeader(nsInfo.Name, nsInfo.Partition)
		if err != nil {
			if err != cluster.ErrKeyNotFound {
				go dc.tryCheckNamespaces()
				return nil, cluster.ErrRegisterServiceUnstable
			}
		}
		if nid == "" && epoch == 0 {
			join = false
			cluster.CoordLog().Infof("my node will create new cluster for namespace: %v since first init: %v",
				nsInfo.GetDesp(), nsConf)
		}
	}
	if forceStandaloneCluster {
		join = false
	}
	localNode, localErr := dc.localNSMgr.InitNamespaceNode(nsConf, raftID, join)
	if localNode != nil {
		if checkErr := localNode.CheckRaftConf(raftID, nsConf); checkErr != nil {
			cluster.CoordLog().Infof("local namespace %v mismatch with the new raft config removing: %v", nsInfo.GetDesp(), checkErr)
			return nil, &cluster.CoordErr{ErrMsg: checkErr.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordLocalErr}
		}
	}
	if localNode == nil {
		cluster.CoordLog().Warningf("local namespace %v init failed: %v", nsInfo.GetDesp(), localErr)
		return nil, cluster.ErrLocalInitNamespaceFailed
	}

	localErr = localNode.SetMagicCode(nsInfo.MagicCode)
	if localErr != nil {
		cluster.CoordLog().Warningf("local namespace %v init magic code failed: %v", nsInfo.GetDesp(), localErr)
		return localNode, cluster.ErrLocalInitNamespaceFailed
	}
	dyConf := &node.NamespaceDynamicConf{}
	localNode.SetDynamicInfo(*dyConf)
	if err := localNode.Start(forceStandaloneCluster); err != nil {
		return nil, cluster.ErrLocalInitNamespaceFailed
	}
	return localNode, nil
}

func (dc *DataCoordinator) RestartAsStandalone(fullNamespace string) error {
	namespace, pid := common.GetNamespaceAndPartition(fullNamespace)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", fullNamespace)
		return ErrNamespaceInvalid
	}
	nsInfo, err := dc.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		return err
	}

	localNs := dc.localNSMgr.GetNamespaceNode(fullNamespace)
	if localNs == nil {
		return ErrNamespaceNotFound
	}
	cluster.CoordLog().Warningf("namespace %v restart as standalone cluster", fullNamespace)
	localNs.Close()
	_, coordErr := dc.updateLocalNamespace(nsInfo, true)
	if coordErr != nil {
		return coordErr.ToErrorType()
	}
	return nil
}

func (dc *DataCoordinator) GetSnapshotSyncInfo(fullNamespace string) ([]common.SnapshotSyncInfo, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNamespace)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", fullNamespace)
		return nil, ErrNamespaceInvalid
	}
	nsInfo, err := dc.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		return nil, err
	}
	var ssiList []common.SnapshotSyncInfo
	for _, nid := range nsInfo.GetISR() {
		node, err := dc.register.GetNodeInfo(nid)
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

func (dc *DataCoordinator) IsRemovingMember(m common.MemberInfo) (bool, error) {
	namespace, pid := common.GetNamespaceAndPartition(m.GroupName)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", m.GroupName)
		return false, ErrNamespaceInvalid
	}
	nsInfo, err := dc.register.GetNamespacePartInfo(namespace, pid)
	if err != nil {
		if err == cluster.ErrKeyNotFound {
			return true, nil
		}
		return false, err
	}

	for nid, rm := range nsInfo.Removings {
		if rm.RemoveReplicaID == m.ID && cluster.ExtractRegIDFromGenID(nid) == m.NodeID {
			return true, nil
		}
	}
	return false, nil
}

func (dc *DataCoordinator) GetNamespaceLeader(fullNS string) (uint64, int64, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNS)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", fullNS)
		return 0, 0, ErrNamespaceInvalid
	}
	nid, epoch, err := dc.register.GetNamespaceLeader(namespace, pid)
	if err != nil {
		if err == cluster.ErrKeyNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if nid == "" {
		return 0, int64(epoch), nil
	}
	regID := cluster.ExtractRegIDFromGenID(nid)
	return regID, int64(epoch), nil
}

func (dc *DataCoordinator) UpdateMeForNamespaceLeader(fullNS string, oldEpoch int64) (int64, error) {
	namespace, pid := common.GetNamespaceAndPartition(fullNS)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", fullNS)
		return 0, ErrNamespaceInvalid
	}
	var rl cluster.RealLeader
	rl.Leader = dc.GetMyID()
	epoch, err := dc.register.UpdateNamespaceLeader(namespace, pid, rl, cluster.EpochType(oldEpoch))
	return int64(epoch), err
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (dc *DataCoordinator) prepareLeavingCluster() {
	cluster.CoordLog().Infof("I am prepare leaving the cluster.")
	allNamespaces, _, _ := dc.register.GetAllNamespaces()
	for _, nsParts := range allNamespaces {
		for _, nsInfo := range nsParts {
			if cluster.FindSlice(nsInfo.RaftNodes, dc.myNode.GetID()) == -1 {
				continue
			}
			localNamespace := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			if localNamespace == nil {
				continue
			}
			// only leader check the follower status
			leader := dc.getNamespaceRaftLeader(nsInfo.GetCopy())
			if leader != dc.GetMyRegID() {
				continue
			}
			for _, newLeader := range nsInfo.GetISR() {
				if newLeader == dc.GetMyID() {
					continue
				}
				dc.transferMyNamespaceLeader(nsInfo.GetCopy(), newLeader, true)
				break
			}
		}
	}
	if dc.register != nil {
		atomic.StoreInt32(&dc.stopping, 1)
		dc.register.Unregister(&dc.myNode)
		dc.register.Stop()
	}

	cluster.CoordLog().Infof("prepare leaving finished.")
	dc.localNSMgr.Stop()
}

func (dc *DataCoordinator) Stats(namespace string, part int) *cluster.CoordStats {
	s := &cluster.CoordStats{}
	s.NsCoordStats = make([]cluster.NamespaceCoordStat, 0)
	if len(namespace) > 0 {
		meta, err := dc.register.GetNamespaceMetaInfo(namespace)
		if err != nil {
			cluster.CoordLog().Infof("failed to get namespace info: %v", err)
			return s
		}
		if part >= 0 {
			nsInfo, err := dc.register.GetNamespacePartInfo(namespace, part)
			if err != nil {
			} else {
				var stat cluster.NamespaceCoordStat
				stat.Name = namespace
				stat.Partition = part
				for _, nid := range nsInfo.RaftNodes {
					stat.ISRStats = append(stat.ISRStats, cluster.ISRStat{HostName: "", NodeID: nid})
				}
				s.NsCoordStats = append(s.NsCoordStats, stat)
			}
		} else {
			for i := 0; i < meta.PartitionNum; i++ {
				nsInfo, err := dc.register.GetNamespacePartInfo(namespace, part)
				if err != nil {
					continue
				}
				var stat cluster.NamespaceCoordStat
				stat.Name = namespace
				stat.Partition = nsInfo.Partition
				for _, nid := range nsInfo.RaftNodes {
					stat.ISRStats = append(stat.ISRStats, cluster.ISRStat{HostName: "", NodeID: nid})
				}
				s.NsCoordStats = append(s.NsCoordStats, stat)
			}
		}
	}
	return s
}

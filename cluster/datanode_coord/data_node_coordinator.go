package datanode_coord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/common"
	node "github.com/youzan/ZanRedisDB/node"
)

var (
	MaxRetryWait         = time.Second * 3
	ErrNamespaceNotReady = cluster.NewCoordErr("namespace node is not ready", cluster.CoordLocalErr)
	ErrNamespaceInvalid  = errors.New("namespace name is invalid")
	ErrNamespaceNotFound = errors.New("namespace is not found")
	// the wait interval while check transferring leader between different partitions
	// to avoid too much partitions do the leader transfer in short time.
	TransferLeaderWait    = time.Second * 20
	CheckUnsyncedInterval = time.Minute * 5
	EnsureJoinCheckWait   = time.Second * 20
	// the wait interval allowed while changing leader in the same raft group
	// to avoid change the leader in the same raft too much
	ChangeLeaderInRaftWait       = time.Minute
	removeNotInMetaPending       = time.Minute
	CheckUnsyncedLearnerInterval = time.Second * 5
)

func ChangeIntervalForTest() {
	TransferLeaderWait = time.Second * 2
	CheckUnsyncedInterval = time.Second * 3
	EnsureJoinCheckWait = time.Second * 2
	ChangeLeaderInRaftWait = time.Second * 2
	removeNotInMetaPending = time.Second * 5
}

const (
	// allow more running since the rsync has a limit on the network traffic
	MaxRaftJoinRunning = 20
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
	learnerRole      string
}

func NewDataCoordinator(cluster string, nodeInfo *cluster.NodeInfo, nsMgr *node.NamespaceMgr) *DataCoordinator {
	coord := &DataCoordinator{
		clusterKey:       cluster,
		register:         nil,
		myNode:           *nodeInfo,
		tryCheckUnsynced: make(chan bool, 1),
		localNSMgr:       nsMgr,
		learnerRole:      nodeInfo.LearnerRole,
	}

	return coord
}

func (dc *DataCoordinator) GetClusterName() string {
	return dc.clusterKey
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
			if dc.localNSMgr != nil {
				err = dc.localNSMgr.SaveMachineRegID(dc.myNode.RegID)
				if err != nil {
					cluster.CoordLog().Errorf("failed to save register id: %v", err)
					return err
				}
			}
		}
		if dc.learnerRole == "" {
			dc.myNode.ID = cluster.GenNodeID(&dc.myNode, "datanode")
		} else {
			dc.myNode.ID = cluster.GenNodeID(&dc.myNode, "datanode-learner-"+dc.learnerRole)
		}
		cluster.CoordLog().Infof("node start with register id: %v, learner role: %v", dc.myNode.RegID, dc.learnerRole)
	}
	return nil
}

func (dc *DataCoordinator) UpdateSyncerWriteOnly(v bool) error {
	return dc.updateRegisterKV("syncer_write_only", v)
}

func (dc *DataCoordinator) GetSyncerWriteOnly() (bool, error) {
	return dc.getRegisterKV("syncer_write_only")
}

func (dc *DataCoordinator) UpdateSyncerNormalInit(v bool) error {
	return dc.updateRegisterKV("syncer_normal_init", v)
}

func (dc *DataCoordinator) GetSyncerNormalInit() (bool, error) {
	return dc.getRegisterKV("syncer_normal_init")
}

func (dc *DataCoordinator) updateRegisterKV(k string, v bool) error {
	if dc.register == nil {
		return errors.New("missing register")
	}
	key := dc.myNode.GetID() + "_kv:" + k
	vstr := "false"
	if v {
		vstr = "true"
	}
	return dc.register.SaveKV(key, vstr)
}

func (dc *DataCoordinator) getRegisterKV(k string) (bool, error) {
	if dc.register == nil {
		return false, errors.New("missing register")
	}
	key := dc.myNode.GetID() + "_kv:" + k
	v, err := dc.register.GetKV(key)
	if err != nil {
		return false, err
	}

	if v == "true" {
		return true, nil
	}
	if v == "false" {
		return false, nil
	}
	return false, fmt.Errorf("invalid value : " + v)
}

func (dc *DataCoordinator) Start() error {
	atomic.StoreInt32(&dc.stopping, 0)
	dc.stopChan = make(chan struct{})
	if dc.register != nil {
		dc.register.Start()
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
		localNsMagics := dc.localNSMgr.CheckLocalNamespaces()
		checkFailed := false
		for ns, localMagic := range localNsMagics {
			namespace, _ := common.GetNamespaceAndPartition(ns)
			if namespace == "" {
				continue
			}
			// check if the magic code mismatch or if already removed by cluster
			nsMeta, err := dc.register.GetNamespaceMetaInfo(namespace)
			if err != nil && err != cluster.ErrKeyNotFound {
				cluster.CoordLog().Warningf("failed to get namespace meta %s from register : %v", ns, err.Error())
				return err
			}
			if err == cluster.ErrKeyNotFound {
				dc.localNSMgr.CleanSharedNsFiles(namespace)
			} else if nsMeta.MagicCode > 0 && localMagic > 0 && localMagic != nsMeta.MagicCode {
				cluster.CoordLog().Errorf("clean left namespace %v data, since magic code not match : %v, %v", ns, localMagic, nsMeta.MagicCode)
				// we can not clean shared namespace data here, since it may only parts of namespace mismatched
				checkFailed = true
			}
		}
		if checkFailed {
			return errors.New("start failed since local data check failed")
		}
	}
	dc.wg.Add(1)
	go dc.watchPD()

	if dc.learnerRole == "" {
		err := dc.loadLocalNamespaceData()
		if err != nil {
			cluster.CoordLog().Infof("load local error : %v", err.Error())
			close(dc.stopChan)
			return err
		}

		dc.wg.Add(1)
		go dc.checkForUnsyncedNamespaces()
	} else if common.IsRoleLogSyncer(dc.learnerRole) {
		dc.loadLocalNamespaceForLearners()
		dc.wg.Add(1)
		go dc.checkForUnsyncedLogSyncers()
	}
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

func (dc *DataCoordinator) loadLocalNamespaceData() error {
	if dc.localNSMgr == nil {
		cluster.CoordLog().Infof("no namespace manager")
		return nil
	}
	namespaceMap, _, err := dc.register.GetAllNamespaces()
	if err != nil {
		if err == cluster.ErrKeyNotFound {
			return nil
		}
		cluster.CoordLog().Infof("load namespace failed: %v", err)
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
			cluster.CoordLog().Debugf("found namespace: %v", nsInfo)
			localNamespace := dc.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			shouldLoad := dc.isNamespaceShouldStart(nsInfo, localNamespace)
			if !shouldLoad {
				if len(nsInfo.GetISR()) >= nsInfo.Replica && localNamespace != nil {
					dc.forceRemoveLocalNamespace(localNamespace)
				}
				if localNamespace != nil {
					cluster.CoordLog().Infof("%v namespace %v ignore to load ", dc.GetMyID(), nsInfo.GetDesp())
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
				cluster.CoordLog().Debugf("%v namespace %v already loaded", dc.GetMyID(), nsInfo.GetDesp())
				continue
			}
			cluster.CoordLog().Infof("mynode %v loading namespace: %v, %v", dc.GetMyID(), nsInfo.GetDesp(), nsInfo)
			if namespaceName == "" {
				continue
			}

			localNamespace, coordErr := dc.updateLocalNamespace(&nsInfo, false)
			if coordErr != nil {
				cluster.CoordLog().Errorf("failed to init/update local namespace %v: %v", nsInfo.GetDesp(), coordErr)
				continue
			}

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

// check if the expected local raft replica id is still in others raft members
func (dc *DataCoordinator) isLocalRaftInRaftGroup(nsInfo *cluster.PartitionMetaInfo, localRID uint64) (bool, error) {
	var lastErr error
	for _, remoteNode := range nsInfo.GetISR() {
		if remoteNode == dc.GetMyID() {
			continue
		}
		nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
		destAddress := net.JoinHostPort(nip, httpPort)
		var rsp []*common.MemberInfo
		code, err := common.APIRequest("GET",
			"http://"+destAddress+common.APIGetMembers+"/"+nsInfo.GetDesp(),
			nil, time.Second*3, &rsp)
		if err != nil {
			cluster.CoordLog().Infof("failed to get members from %v for namespace: %v, %v", destAddress, nsInfo.GetDesp(), err)
			if code == http.StatusNotFound {
				lastErr = ErrNamespaceNotFound
			} else {
				lastErr = err
			}
			continue
		}

		for _, m := range rsp {
			if m.NodeID == dc.GetMyRegID() && m.ID == localRID {
				cluster.CoordLog().Infof("from %v for namespace: %v, node is still in raft: %v", destAddress, nsInfo.GetDesp(), m)
				return true, nil
			}
		}
	}
	return false, lastErr
}

func (dc *DataCoordinator) isNamespaceShouldStart(nsInfo cluster.PartitionMetaInfo, localNs *node.NamespaceNode) bool {
	// it may happen that the node marked as removing can not be removed because
	// the raft group has not enough quorum to do the proposal to change the configure.
	// In this way we need start the removing node to join the raft group and then
	// the leader can remove the node from the raft group, and then we can safely remove
	// the removing node finally
	shouldLoad := cluster.FindSlice(nsInfo.RaftNodes, dc.GetMyID()) != -1
	if !shouldLoad {
		return false
	}
	var localRID uint64
	// if local namespace is started, we check if it is the replica we actually need.
	// If no local namespace, we check if we still need to start the new node locally.
	if localNs != nil {
		localRID = localNs.GetRaftID()
		if localRID != nsInfo.RaftIDs[dc.GetMyID()] {
			cluster.CoordLog().Infof("local node raft id %v not match namespace %v meta: %v",
				localRID, nsInfo.GetDesp(), nsInfo.RaftIDs)
			// check if in other nodes raft
			inRaft, err := dc.isLocalRaftInRaftGroup(&nsInfo, localRID)
			if inRaft || err == ErrNamespaceNotFound {
				cluster.CoordLog().Infof("node %v-%v should join namespace %v since still in others raft",
					dc.GetMyID(), localRID, nsInfo.GetDesp())
				return true
			}
			return false
		}
	} else {
		localRID = nsInfo.RaftIDs[dc.GetMyID()]
	}
	rm, ok := nsInfo.Removings[dc.GetMyID()]
	if !ok {
		return true
	}
	if rm.RemoveReplicaID != localRID {
		return true
	}

	inRaft, err := dc.isLocalRaftInRaftGroup(&nsInfo, localRID)
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
		cluster.CoordLog().Infof("no any meta info for this namespace: %v, rid: %v", nsInfo.GetDesp(), localNamespace.GetRaftID())
	}

	inRaft, err := dc.isLocalRaftInRaftGroup(&nsInfo, localNamespace.GetRaftID())
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

func (dc *DataCoordinator) transferMyNamespaceLeader(nsInfo *cluster.PartitionMetaInfo, nid string, force bool, checkAll bool) bool {
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
		if time.Now().UnixNano()-nsNode.GetLastLeaderChangedTime() < ChangeLeaderInRaftWait.Nanoseconds() {
			return false
		}
		if !dc.isReplicaReadyForRaft(nsNode, toRaftID, nid, checkAll) {
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
func (dc *DataCoordinator) isReplicaReadyForRaft(nsNode *node.NamespaceNode, toRaftID uint64, nodeID string, checkAll bool) bool {
	if nsNode.IsReady() && nsNode.Node.IsReplicaRaftReady(toRaftID) {
		if checkAll {
			nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(nodeID)
			code, err := common.APIRequest("GET",
				"http://"+net.JoinHostPort(nip, httpPort)+common.APINodeAllReady,
				nil, time.Second*10, nil)
			if err != nil {
				cluster.CoordLog().Infof("not ready from %v for transfer leader: %v, %v", nip, code, err.Error())
				return false
			}
		}
		return true
	}
	if nsNode.IsReady() {
		stats := nsNode.Node.GetRaftStatus()
		cluster.CoordLog().Infof("namespace %v raft status still not ready for node:%v, %v",
			nsNode.FullName(), toRaftID, stats)
	}
	return false
}

type pendingRemoveInfo struct {
	ts time.Time
	m  common.MemberInfo
}

func checkRemoveNode(pendings map[uint64]pendingRemoveInfo,
	newestReplicaInfo *cluster.PartitionReplicaInfo,
	m *common.MemberInfo, isLearner bool) (bool, bool) {
	found := false
	if isLearner {
		for _, nids := range newestReplicaInfo.LearnerNodes {
			for _, nid := range nids {
				rid := newestReplicaInfo.RaftIDs[nid]
				regNodeID := cluster.ExtractRegIDFromGenID(nid)
				if m.ID == rid && m.NodeID == regNodeID {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	} else {
		for _, nid := range newestReplicaInfo.RaftNodes {
			rid := newestReplicaInfo.RaftIDs[nid]
			if m.ID == rid {
				found = true
				if m.NodeID != cluster.ExtractRegIDFromGenID(nid) {
					cluster.CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
				}
				break
			}
		}
	}
	if found {
		delete(pendings, m.ID)
	} else {
		cluster.CoordLog().Infof("raft node %v not found in meta: %v", m, newestReplicaInfo.LearnerNodes)
		if pendRemove, ok := pendings[m.ID]; ok {
			if !pendRemove.m.IsEqual(m) {
				pendings[m.ID] = pendingRemoveInfo{
					ts: time.Now(),
					m:  *m,
				}
			} else if time.Since(pendRemove.ts) > removeNotInMetaPending {
				cluster.CoordLog().Infof("pending removing node %v finally removed since not in meta", pendRemove)
				return true, false
			}
		} else {
			pendings[m.ID] = pendingRemoveInfo{
				ts: time.Now(),
				m:  *m,
			}
		}
	}
	return false, found
}

func (dc *DataCoordinator) checkForUnsyncedNamespaces() {
	ticker := time.NewTicker(CheckUnsyncedInterval)
	cluster.CoordLog().Infof("%v begin to check for unsynced namespace", dc.GetMyID())
	defer cluster.CoordLog().Infof("%v check for unsynced namespace quit", dc.GetMyID())
	defer dc.wg.Done()

	pendingRemovings := make(map[string]map[uint64]pendingRemoveInfo)
	// avoid transfer too much partitions in the same time
	lastTransferCheckedTime := time.Now()
	doWork := func() {
		if atomic.LoadInt32(&dc.stopping) == 1 {
			return
		}
		// try load local namespace if any namespace raft group changed
		err := dc.loadLocalNamespaceData()
		if err != nil {
			cluster.CoordLog().Infof("%v load local error : %v", dc.GetMyID(), err.Error())
		}

		// check local namespaces with cluster to remove the unneed data
		if dc.localNSMgr == nil {
			return
		}
		tmpChecks := dc.localNSMgr.GetNamespaces()
		allIndexSchemas := make(map[string]map[string]*common.IndexSchema)
		allNamespaces, _, err := dc.register.GetAllNamespaces()
		if err != nil {
			cluster.CoordLog().Infof("error while check sync: %v", err.Error())
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
				cluster.CoordLog().Infof("got namespace %v meta failed: %v", name, err)
				if err == cluster.ErrKeyNotFound {
					cluster.CoordLog().Infof("the namespace should be clean since not found in register: %v", name)
					_, err = dc.register.GetNamespaceMetaInfo(namespace)
					if err == cluster.ErrKeyNotFound {
						dc.forceRemoveLocalNamespace(localNamespace)
						dc.localNSMgr.CleanSharedNsFiles(namespace)
					}
				} else {
					dc.tryCheckNamespacesIn(time.Second * 5)
					return
				}
				dc.tryCheckNamespacesIn(time.Second * 5)
				continue
			}

			localRID := localNamespace.GetRaftID()
			if dc.isNamespaceShouldStop(*namespaceMeta, localNamespace) {
				dc.forceRemoveLocalNamespace(localNamespace)
				continue
			}
			leader := dc.getNamespaceRaftLeader(namespaceMeta)
			isrList := namespaceMeta.GetISR()
			if localRID != namespaceMeta.RaftIDs[dc.GetMyID()] {
				cluster.CoordLog().Infof("local raft id %v not match meta, the namespace should be clean : %v", localRID, namespaceMeta)
				if len(isrList) > 0 {
					// check if in other nodes raft
					inRaft, err := dc.isLocalRaftInRaftGroup(namespaceMeta, localRID)
					if inRaft || err == ErrNamespaceNotFound {
						cluster.CoordLog().Infof("node %v-%v for namespace %v is still in others raft",
							dc.GetMyID(), localRID, namespaceMeta.GetDesp())
						inRaft = true
					}
					if inRaft {
						dc.removeLocalNamespaceFromRaft(localNamespace)
					} else {
						// since this node will be joined in other raft id, maybe we can just stop without clean old data
						if localNamespace != nil {
							localNamespace.Close()
						}
					}
				}
				continue
			}
			if leader == 0 {
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
				if time.Since(lastTransferCheckedTime) >= TransferLeaderWait {
					// removing node should transfer leader immediately since others partitions may wait for ready ,
					// so it may never all ready for transfer. we transfer only check local partition.
					_, removed := namespaceMeta.Removings[dc.GetMyID()]
					done = dc.transferMyNamespaceLeader(namespaceMeta, isrList[0], false, !removed)
					lastTransferCheckedTime = time.Now()
				}
				if !done {
					dc.tryCheckNamespacesIn(TransferLeaderWait)
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
				if time.Since(lastTransferCheckedTime) >= TransferLeaderWait {
					done = dc.transferMyNamespaceLeader(namespaceMeta, isrList[0], false, true)
					lastTransferCheckedTime = time.Now()
				}
				if !done {
					dc.tryCheckNamespacesIn(TransferLeaderWait)
				}
				continue
			}
			// check if any replica is not joined to members
			anyWaitingJoin := false
			for _, nid := range namespaceMeta.GetISR() {
				rid := namespaceMeta.RaftIDs[nid]
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
				dc.tryCheckNamespacesIn(time.Second * 5)
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
					dc.tryCheckNamespacesIn(time.Second)
				}
				delete(pendingRemovings, namespaceMeta.GetDesp())
				continue
			}
			// begin handle the member which may be removed but still in raft group
			namespaceMeta.PartitionReplicaInfo = *newestReplicaInfo
			lrns := localNamespace.GetLearners()
			for _, lrn := range lrns {
				needRemove, found := checkRemoveNode(pendings, newestReplicaInfo, lrn, true)
				if needRemove {
					cluster.CoordLog().Infof("pending removing learner %v finally removed since not in meta", lrn)
					dc.removeNamespaceRaftMember(namespaceMeta, lrn)
				}
				// some learner is not found, we need check later
				if !found {
					dc.tryCheckNamespacesIn(time.Second)
				}
			}
			for _, m := range members {
				needRemove, found := checkRemoveNode(pendings, newestReplicaInfo, m, false)
				if needRemove {
					dc.removeNamespaceRaftMember(namespaceMeta, m)
				}
				if !found {
					dc.tryCheckNamespacesIn(time.Second)
				} else {
					// still in raft group, check if in removing
					// removing node can be removed immediately without wait pending
					for nid, removing := range newestReplicaInfo.Removings {
						isFullStable = false
						if m.ID == removing.RemoveReplicaID && m.NodeID == cluster.ExtractRegIDFromGenID(nid) {
							if m.NodeID == leader {
								cluster.CoordLog().Infof("raft leader member %v is marked as removing in meta: %v, waiting transfer", m, newestReplicaInfo.Removings)
								// leader should not remove self, otherwise there maybe sometimes no leader for rw
								// so leader need wait transfer self to others and wait removed by other leader
								dc.tryCheckNamespacesIn(TransferLeaderWait)
							} else {
								cluster.CoordLog().Infof("raft member %v is marked as removing in meta: %v", m, newestReplicaInfo.Removings)
								dc.removeNamespaceRaftMember(namespaceMeta, m)
							}
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

	nsChangedChan := dc.register.GetNamespacesNotifyChan()
	for {
		select {
		case <-dc.stopChan:
			return
		case <-dc.tryCheckUnsynced:
			doWork()
			time.Sleep(time.Millisecond * 100)
		case <-ticker.C:
			doWork()
		case <-nsChangedChan:
			cluster.CoordLog().Infof("trigger check by namespace changed")
			doWork()
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (dc *DataCoordinator) forceRemoveLocalNamespace(localNamespace *node.NamespaceNode) {
	if localNamespace == nil {
		return
	}
	cluster.CoordLog().Infof("force remove local data: %v", localNamespace.FullName())
	err := localNamespace.Destroy()
	if err != nil {
		cluster.CoordLog().Infof("failed to force remove local data: %v", err)
	}
}

func (dc *DataCoordinator) removeLocalNamespaceFromRaft(localNamespace *node.NamespaceNode) *cluster.CoordErr {
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

	return nil
}

func (dc *DataCoordinator) getRaftAddrForNode(nid string) (string, *cluster.CoordErr) {
	node, err := dc.register.GetNodeInfo(nid)
	if err != nil {
		return "", &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	}
	return node.RaftTransportAddr, nil
}

func (dc *DataCoordinator) prepareNamespaceConf(nsInfo *cluster.PartitionMetaInfo, raftID uint64,
	join bool, forceStandaloneCluster bool) (*node.NamespaceConfig, *cluster.CoordErr) {
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
	if nsInfo.DataVersion != "" {
		nsConf.DataVersion = nsInfo.DataVersion
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
	if !join && len(nsConf.RaftGroupConf.SeedNodes) <= nsInfo.Replica/2 {
		cluster.CoordLog().Warningf("seed nodes for namespace %v not enough: %v", nsInfo.GetDesp(), nsConf.RaftGroupConf)
		// we should allow single node as new raft cluster while in disaster re-init mode.
		// In this case, we only have one seed node while init. (Notice, it may not allow write while init
		// since the raft node is small than half of the replicator )
		if !forceStandaloneCluster {
			return nil, cluster.ErrNamespaceConfInvalid
		}
	}
	return nsConf, nil
}

func (dc *DataCoordinator) requestJoinNamespaceGroup(raftID uint64, nsInfo *cluster.PartitionMetaInfo,
	localNamespace *node.NamespaceNode, remoteNode string, joinAsLearner bool) error {
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
	uri := "http://" + net.JoinHostPort(nip, httpPort) + common.APIAddNode
	if joinAsLearner {
		uri = "http://" + net.JoinHostPort(nip, httpPort) + common.APIAddLearnerNode
	}
	_, err := common.APIRequest("POST",
		uri,
		bytes.NewReader(d), time.Second*3, nil)
	if err != nil {
		cluster.CoordLog().Infof("failed to request join namespace: %v", err)
		return err
	}
	return nil
}

func (dc *DataCoordinator) tryCheckNamespacesIn(wait time.Duration) {
	if len(dc.tryCheckUnsynced) > 0 {
		return
	}
	go func() {
		time.Sleep(wait)
		dc.tryCheckNamespaces()
	}()
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
	if myRunning > MaxRaftJoinRunning {
		cluster.CoordLog().Infof("catching too much running: %v", myRunning)
		dc.tryCheckNamespaces()
		return cluster.ErrCatchupRunningBusy
	}

	dyConf := &node.NamespaceDynamicConf{
		nsInfo.Replica,
	}
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
	requestJoined := make(map[string]bool)
	for time.Since(startCheck) < EnsureJoinCheckWait {
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
			if localNamespace.IsNsNodeFullReady(firstLoad) {
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
			if !dc.isNamespaceShouldStart(nsInfo, localNamespace) {
				return cluster.ErrNamespaceExiting
			}
			// TODO: check if the local node is in the progress of starting or applying the snapshot
			// in this case, we just wait the start finished.
			if _, ok := requestJoined[remote]; !ok {
				err := dc.requestJoinNamespaceGroup(raftID, &nsInfo, localNamespace, remote, false)
				if err == nil {
					requestJoined[remote] = true
				}
			}
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
		cluster.CoordLog().Warningf("namespace %v has no raft id for local: %v", nsInfo.GetDesp(), nsInfo.RaftIDs)
		return nil, cluster.ErrNamespaceConfInvalid
	}

	// handle if we need join the existing cluster
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
			cluster.CoordLog().Infof("my node will create new cluster for namespace: %v since first init",
				nsInfo.GetDesp())
		}
	}
	if forceStandaloneCluster {
		join = false
	}
	nsConf, err := dc.prepareNamespaceConf(nsInfo, raftID, join, forceStandaloneCluster)
	if err != nil {
		go dc.tryCheckNamespaces()
		cluster.CoordLog().Warningf("prepare join namespace %v failed: %v", nsInfo.GetDesp(), err)
		return nil, err
	}

	localNode, localErr := dc.localNSMgr.InitNamespaceNode(nsConf, raftID, join)
	if localNode != nil {
		if checkErr := localNode.CheckRaftConf(raftID, nsConf); checkErr != nil {
			cluster.CoordLog().Infof("local namespace %v mismatch with the new raft config removing: %v", nsInfo.GetDesp(), checkErr)
			return nil, &cluster.CoordErr{ErrMsg: checkErr.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordLocalErr}
		}
	}
	if localNode == nil || localErr != nil {
		cluster.CoordLog().Warningf("local namespace %v init failed: %v", nsInfo.GetDesp(), localErr)
		if localNode == nil {
			return nil, cluster.ErrLocalInitNamespaceFailed
		}
		if localErr != node.ErrNamespaceAlreadyExist {
			return nil, cluster.ErrLocalInitNamespaceFailed
		}
	}

	localErr = localNode.SetMagicCode(nsInfo.MagicCode)
	if localErr != nil {
		cluster.CoordLog().Warningf("local namespace %v init magic code failed: %v", nsInfo.GetDesp(), localErr)
		if localErr == node.ErrLocalMagicCodeConflict {
			dc.forceRemoveLocalNamespace(localNode)
		}
		return localNode, cluster.ErrLocalInitNamespaceFailed
	}
	dyConf := &node.NamespaceDynamicConf{
		nsConf.Replicator,
	}
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
	// wait delete from namespace manager
	time.Sleep(time.Second)

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

func (dc *DataCoordinator) UpdateMeForNamespaceLeader(fullNS string) (bool, error) {
	if dc.learnerRole != "" {
		cluster.CoordLog().Warningf("should never update me for leader in learner role: %v", fullNS)
		return false, nil
	}
	namespace, pid := common.GetNamespaceAndPartition(fullNS)
	if namespace == "" {
		cluster.CoordLog().Warningf("namespace invalid: %v", fullNS)
		return false, ErrNamespaceInvalid
	}
	nid, oldEpoch, err := dc.register.GetNamespaceLeader(namespace, pid)
	if err != nil {
		if err != cluster.ErrKeyNotFound {
			return false, err
		}
	}
	var rl cluster.RealLeader
	rl.Leader = dc.GetMyID()
	if nid != "" {
		regID := cluster.ExtractRegIDFromGenID(nid)
		if regID == dc.GetMyRegID() && nid == rl.Leader {
			return false, nil
		}
	}
	_, err = dc.register.UpdateNamespaceLeader(namespace, pid, rl, cluster.EpochType(oldEpoch))
	return true, err
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
				done := dc.transferMyNamespaceLeader(nsInfo.GetCopy(), newLeader, true, false)
				if done {
					break
				}
			}
		}
	}
	if dc.register != nil {
		dc.register.Unregister(&dc.myNode)
		dc.register.Stop()
	}

	cluster.CoordLog().Infof("prepare leaving finished.")
	if dc.localNSMgr != nil {
		dc.localNSMgr.Stop()
	}
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

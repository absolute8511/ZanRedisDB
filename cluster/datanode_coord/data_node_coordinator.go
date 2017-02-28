package datanode_coord

import (
	"bytes"
	"encoding/json"
	"errors"
	. "github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	node "github.com/absolute8511/ZanRedisDB/node"
	"net"
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
				CoordLog().Infof("pd leader changed: %v", n)
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

func (self *DataCoordinator) forceCleanNamespaceData(name string) *CoordErr {
	// check if any data on local and try remove
	localErr := self.localNSMgr.ForceDeleteNamespaceData(name)
	if localErr != nil {
		if !os.IsNotExist(localErr) {
			CoordLog().Infof("delete namespace %v local data failed : %v", name, localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		}
	}
	return nil
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
	for namespaceName, namespaceParts := range namespaceMap {
		CoordLog().Infof("loading namespace %v, %v", namespaceName, namespaceParts)
		for _, nsInfo := range namespaceParts {
			CoordLog().Infof("namespace: %v", nsInfo)
			shouldLoad := FindSlice(nsInfo.RaftNodes, self.myNode.GetID()) != -1
			if !shouldLoad {
				if len(nsInfo.RaftNodes) >= nsInfo.Replica {
					self.forceCleanNamespaceData(nsInfo.GetDesp())
				}
				continue
			}
			CoordLog().Infof("loading namespace: %v", nsInfo.GetDesp())
			localNamespace := self.localNSMgr.GetNamespaceNode(nsInfo.GetDesp())
			if localNamespace != nil {
				// already loaded
				self.ensureJoinNamespaceGroup(&nsInfo, localNamespace)
				continue
			}
			if namespaceName == "" {
				continue
			}
			checkErr := self.checkLocalNamespaceMagicCode(&nsInfo, true)
			if checkErr != nil {
				CoordLog().Errorf("failed to check namespace :%v, err:%v", nsInfo.GetDesp(), checkErr)
				continue
			}

			localNamespace, coordErr := self.updateLocalNamespace(&nsInfo)
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
			go self.ensureJoinNamespaceGroup(&nsInfo, localNamespace)
		}
	}
	return nil
}

func (self *DataCoordinator) checkAndFixLocalNamespaceData(nsInfo *PartitionMetaInfo, localNamespace *node.NamespaceNode) error {
	return nil
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
				CoordLog().Warningf("namespace invalid: %v", name)
				continue
			}
			namespace := splits[0]
			pid, err := strconv.Atoi(splits[1])
			if err != nil {
				CoordLog().Warningf("namespace invalid: %v, %v", name, splits)
				continue
			}
			namespaceMeta, err := self.register.GetNamespacePartInfo(namespace, pid)
			if err != nil {
				if err == ErrKeyNotFound {
					CoordLog().Infof("the namespace should be clean since not found in register: %v", name)
					_, err = self.register.GetNamespaceMetaInfo(namespace)
					if err == ErrKeyNotFound {
						self.removeLocalNamespace(name, true)
					}
				}
				continue
			}
			if FindSlice(namespaceMeta.RaftNodes, self.myNode.GetID()) == -1 {
				if len(namespaceMeta.RaftNodes) > 0 {
					CoordLog().Infof("the namespace should be clean since not relevance to me: %v", namespaceMeta)
					self.removeLocalNamespace(name, true)
				}
			} else if len(namespaceMeta.RaftNodes) >= namespaceMeta.Replica {
				leader := self.getNamespaceRaftLeader(namespaceMeta)
				if leader != self.GetMyRegID() {
					continue
				}
				if namespaceMeta.RaftNodes[0] != self.GetMyID() {
					// the raft leader check if I am the expected sharding leader,
					// if not, try to transfer the leader to expected node. We need do this
					// because we should make all the sharding leaders balanced on
					// all the cluster nodes.
					self.transferMyNamespaceLeader(namespaceMeta, namespaceMeta.RaftNodes[0])
				} else {
					members := self.getNamespaceRaftMembers(namespaceMeta)
					for _, m := range members {
						found := false
						for nid, rid := range namespaceMeta.RaftIDs {
							if m.ID == rid {
								found = true
								if m.NodeID != ExtractRegIDFromGenID(nid) {
									CoordLog().Infof("found raft member %v mismatch the replica node: %v", m, nid)
								}
								break
							}
						}
						if !found {
							CoordLog().Infof("raft member %v not found in meta: %v", m, namespaceMeta.RaftNodes)
							self.removeNamespaceRaftMember(namespaceMeta, m)
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

func (self *DataCoordinator) removeLocalNamespace(name string, removeData bool) *CoordErr {
	if removeData {
		CoordLog().Infof("removing namespace : %v", name)
		// check if any data on local and try remove
		self.forceCleanNamespaceData(name)
	} else {
		localNamespace := self.localNSMgr.GetNamespaceNode(name)
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
		return "", &CoordErr{err.Error(), RpcNoErr, CoordRegisterErr}
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
	nsConf.RaftGroupConf.GroupID = uint64(nsInfo.MinGID) + uint64(nsInfo.Partition)
	nsConf.RaftGroupConf.SeedNodes = make([]node.ReplicaInfo, 0)
	for _, nid := range nsInfo.RaftNodes {
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
	err := common.APIRequest("POST",
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

func (self *DataCoordinator) ensureJoinNamespaceGroup(nsInfo *PartitionMetaInfo,
	localNamespace *node.NamespaceNode) *CoordErr {
	// check if in local raft group
	myRunning := atomic.AddInt32(&self.catchupRunning, 1)
	defer atomic.AddInt32(&self.catchupRunning, -1)
	if myRunning > MAX_RAFT_JOIN_RUNNING {
		CoordLog().Infof("catching too much running: %v", myRunning)
		self.tryCheckNamespaces()
		return ErrCatchupRunningBusy
	}

	CoordLog().Infof("ensure local namespace %v join raft group", nsInfo.GetDesp())
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
	for retry < 2*len(nsInfo.RaftNodes) {
		mems := localNamespace.GetMembers()
		alreadyJoined := false
		for _, m := range mems {
			if m.NodeID == self.GetMyRegID() &&
				m.GroupName == nsInfo.GetDesp() &&
				m.ID == raftID {
				CoordLog().Infof("namespace %v is in raft group, wait sync", nsInfo.GetDesp())
				alreadyJoined = true
			}
		}
		if alreadyJoined {
			if localNamespace.IsRaftSynced() {
				CoordLog().Infof("namespace %v is synced", nsInfo.GetDesp())
				joinErr = nil
				break
			}
			CoordLog().Infof("namespace %v still waiting raft synced", nsInfo.GetDesp())
			select {
			case <-self.stopChan:
				return ErrNamespaceExiting
			case <-time.After(time.Second):
			}
			joinErr = ErrNamespaceWaitingSync
		} else {
			joinErr = ErrNamespaceWaitingSync
			remote := nsInfo.RaftNodes[retry%len(nsInfo.RaftNodes)]
			retry++
			if remote == self.GetMyID() {
				continue
			}
			select {
			case <-self.stopChan:
				return ErrNamespaceExiting
			case <-time.After(time.Second):
			}
			self.requestJoinNamespaceGroup(raftID, nsInfo, localNamespace, remote)
			select {
			case <-self.stopChan:
				return ErrNamespaceExiting
			case <-time.After(time.Second):
			}
		}
	}
	if joinErr != nil {
		self.tryCheckNamespaces()
		CoordLog().Infof("local namespace join failed: %v, retry later: %v", joinErr, nsInfo.GetDesp())
	} else {
		CoordLog().Infof("local namespace join done: %v", nsInfo.GetDesp())
	}
	return joinErr
}

func (self *DataCoordinator) updateLocalNamespace(nsInfo *PartitionMetaInfo) (*node.NamespaceNode, *CoordErr) {
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

	localNode, _ := self.localNSMgr.InitNamespaceNode(nsConf, raftID)
	if localNode != nil {
		if checkErr := localNode.CheckRaftConf(raftID, nsConf); checkErr != nil {
			CoordLog().Infof("local namespace %v mismatch with the new raft config removing: %v", nsInfo.GetDesp(), checkErr)
			self.localNSMgr.ForceDeleteNamespaceData(nsConf.Name)
			localNode, _ = self.localNSMgr.InitNamespaceNode(nsConf, raftID)
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
	localNode.Start()
	return localNode, nil
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
			if len(nsInfo.RaftNodes)-1 <= nsInfo.Replica/2 {
				CoordLog().Infof("The isr nodes in namespace %v is not enough while leaving: %v",
					nsInfo.GetDesp(), nsInfo.RaftNodes)
			}
		}
	}
	CoordLog().Infof("prepare leaving finished.")
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

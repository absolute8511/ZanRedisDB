package pdnode_coord

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/common"
)

var (
	ErrAlreadyExist    = errors.New("already exist")
	ErrNotLeader       = errors.New("Not leader")
	ErrClusterUnstable = errors.New("the cluster is unstable")

	ErrLeaderNodeLost            = cluster.NewCoordErr("leader node is lost", cluster.CoordTmpErr)
	ErrNodeNotFound              = cluster.NewCoordErr("node not found", cluster.CoordCommonErr)
	ErrNodeUnavailable           = cluster.NewCoordErr("No node is available for namespace", cluster.CoordTmpErr)
	ErrNamespaceRaftEnough       = cluster.NewCoordErr("the namespace isr and catchup nodes are enough", cluster.CoordTmpErr)
	ErrClusterNodeRemoving       = cluster.NewCoordErr("the node is mark as removed", cluster.CoordTmpErr)
	ErrNamespaceNodeConflict     = cluster.NewCoordErr("the namespace node info is conflicted", cluster.CoordClusterErr)
	ErrNamespaceRaftIDNotFound   = cluster.NewCoordErr("the namespace raft id is not found", cluster.CoordClusterErr)
	ErrNamespaceReplicaNotEnough = cluster.NewCoordErr("the replicas in the namespace is not enough", cluster.CoordTmpErr)
	ErrNamespaceMigrateWaiting   = cluster.NewCoordErr("the migrate is waiting", cluster.CoordTmpErr)
)

var (
	waitMigrateInterval            = time.Minute * 16
	waitRemoveRemovingNodeInterval = time.Minute * 5
	nsCheckInterval                = time.Minute
	nsCheckLearnerInterval         = time.Second * 10
	balanceCheckInterval           = time.Minute * 10
	checkRemovingNodeInterval      = time.Minute
)

func ChangeIntervalForTest() {
	waitMigrateInterval = time.Second * 10
	waitRemoveRemovingNodeInterval = time.Second * 5
	nsCheckInterval = time.Second
	balanceCheckInterval = time.Second * 5
	checkRemovingNodeInterval = time.Second * 5
}

type PDCoordinator struct {
	register               cluster.PDRegister
	balanceWaiting         int32
	clusterKey             string
	myNode                 cluster.NodeInfo
	leaderNode             cluster.NodeInfo
	nodesMutex             sync.RWMutex
	dataNodes              map[string]cluster.NodeInfo
	learnerNodes           map[string]cluster.NodeInfo
	removingNodes          map[string]string
	nodesEpoch             int64
	checkNamespaceFailChan chan cluster.NamespaceNameInfo
	stopChan               chan struct{}
	wg                     sync.WaitGroup
	monitorChan            chan struct{}
	isClusterUnstable      int32
	isUpgrading            int32
	dpm                    *DataPlacement
	doChecking             int32
	autoBalance            int32
	stableNodeNum          int32
	dataDir                string
	learnerRole            string
	filterNamespaces       map[string]bool
}

func NewPDCoordinator(clusterID string, n *cluster.NodeInfo, opts *cluster.Options) *PDCoordinator {
	if n.LearnerRole == "" {
		n.ID = cluster.GenNodeID(n, "pd")
	} else {
		n.ID = cluster.GenNodeID(n, "pd-learner-"+n.LearnerRole)
	}
	coord := &PDCoordinator{
		clusterKey:             clusterID,
		myNode:                 *n,
		register:               nil,
		dataNodes:              make(map[string]cluster.NodeInfo),
		learnerNodes:           make(map[string]cluster.NodeInfo),
		removingNodes:          make(map[string]string),
		checkNamespaceFailChan: make(chan cluster.NamespaceNameInfo, 3),
		stopChan:               make(chan struct{}),
		monitorChan:            make(chan struct{}),
		learnerRole:            n.LearnerRole,
		filterNamespaces:       make(map[string]bool),
	}
	coord.dpm = NewDataPlacement(coord)
	if opts != nil {
		coord.dpm.SetBalanceInterval(opts.BalanceStart, opts.BalanceEnd)
		if opts.AutoBalanceAndMigrate {
			coord.autoBalance = 1
		}
		coord.dataDir = opts.DataDir
		nss := strings.Split(opts.FilterNamespaces, ",")
		for _, ns := range nss {
			if len(ns) > 0 {
				coord.filterNamespaces[ns] = true
			}
		}
	}
	return coord
}

func (pdCoord *PDCoordinator) SetBalanceInterval(start, end int) {
	pdCoord.dpm.SetBalanceInterval(start, end)
}

func (pdCoord *PDCoordinator) SetRegister(r cluster.PDRegister) {
	pdCoord.register = r
}

func (pdCoord *PDCoordinator) AutoBalanceEnabled() bool {
	return atomic.LoadInt32(&pdCoord.autoBalance) == 1
}

func (pdCoord *PDCoordinator) Start() error {
	if pdCoord.register != nil {
		pdCoord.register.InitClusterID(pdCoord.clusterKey)
		pdCoord.register.Start()
		err := pdCoord.register.Register(&pdCoord.myNode)
		if err != nil {
			cluster.CoordLog().Warningf("failed to register pd coordinator: %v", err)
			return err
		}
	}

	if pdCoord.learnerRole != "" {
		pdCoord.wg.Add(1)
		go func() {
			defer pdCoord.wg.Done()
			pdCoord.handleDataNodes(pdCoord.stopChan, false)
		}()
		pdCoord.wg.Add(1)
		go func() {
			defer pdCoord.wg.Done()
			pdCoord.checkNamespacesForLearner(pdCoord.stopChan)
		}()
	} else {
		pdCoord.wg.Add(1)
		go pdCoord.handleLeadership()
	}
	return nil
}

func (pdCoord *PDCoordinator) Stop() {
	close(pdCoord.stopChan)
	if pdCoord.register != nil {
		pdCoord.register.Unregister(&pdCoord.myNode)
		pdCoord.register.Stop()
	}
	pdCoord.wg.Wait()
	cluster.CoordLog().Infof("coordinator stopped.")
}

func (pdCoord *PDCoordinator) handleLeadership() {
	defer pdCoord.wg.Done()
	leaderChan := make(chan *cluster.NodeInfo)
	if pdCoord.register != nil {
		go pdCoord.register.AcquireAndWatchLeader(leaderChan, pdCoord.stopChan)
	}
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			cluster.CoordLog().Errorf("panic %s:%v", buf, e)
		}

		cluster.CoordLog().Warningf("leadership watch exit.")
		if pdCoord.monitorChan != nil {
			close(pdCoord.monitorChan)
			pdCoord.monitorChan = nil
		}
	}()
	for {
		select {
		case l, ok := <-leaderChan:
			if !ok {
				cluster.CoordLog().Warningf("leader chan closed.")
				return
			}
			if l == nil {
				atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
				cluster.CoordLog().Warningf("leader is lost.")
				continue
			}
			if l.GetID() != pdCoord.leaderNode.GetID() {
				cluster.CoordLog().Infof("leader changed from %v to %v", pdCoord.leaderNode, *l)
				pdCoord.leaderNode = *l
				if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
					// remove watchers.
					if pdCoord.monitorChan != nil {
						close(pdCoord.monitorChan)
					}
					pdCoord.monitorChan = make(chan struct{})
				}
				pdCoord.notifyLeaderChanged(pdCoord.monitorChan)
			}
			if pdCoord.leaderNode.GetID() == "" {
				cluster.CoordLog().Warningf("leader is missing.")
			}
		}
	}
}

func (pdCoord *PDCoordinator) saveClusterToFile(newNamespaces map[string]map[int]cluster.PartitionMetaInfo) {
	cluster.CoordLog().Infof("begin save namespace meta ")
	d, _ := json.Marshal(newNamespaces)
	dataDir := pdCoord.dataDir
	if dataDir == "" {
		var err error
		dataDir, err = ioutil.TempDir("", "zankv-meta")
		if err != nil {
			cluster.CoordLog().Infof("init temp dir failed: %v", err.Error())
			return
		}
	}
	prefix := strconv.Itoa(int(time.Now().UnixNano()))
	prefix = path.Join(dataDir, prefix)
	ioutil.WriteFile(prefix+".meta.ns", d, common.FILE_PERM)
	meta, err := pdCoord.register.GetClusterMetaInfo()
	if err != nil {
		cluster.CoordLog().Infof("get cluster meta failed: %v", err.Error())
	} else {
		d, _ := json.Marshal(meta)
		ioutil.WriteFile(prefix+".meta.cluster", d, common.FILE_PERM)
	}
	for ns := range newNamespaces {
		schemas, err := pdCoord.register.GetNamespaceSchemas(ns)
		if err != nil {
			cluster.CoordLog().Infof("namespace schemas failed: %v", err.Error())
			continue
		}
		d, _ = json.Marshal(schemas)
		ioutil.WriteFile(prefix+".schemas."+ns, d, common.FILE_PERM)
	}
	cluster.CoordLog().Infof("namespace meta saved to : %v", dataDir)
}

func (pdCoord *PDCoordinator) notifyLeaderChanged(monitorChan chan struct{}) {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("I am slave (%v). Leader is: %v", pdCoord.myNode, pdCoord.leaderNode)
		pdCoord.nodesMutex.Lock()
		pdCoord.removingNodes = make(map[string]string)
		pdCoord.nodesMutex.Unlock()

		pdCoord.wg.Add(1)
		go func() {
			defer pdCoord.wg.Done()
			pdCoord.handleDataNodes(monitorChan, false)
		}()

		return
	}
	cluster.CoordLog().Infof("I am master now.")
	// reload namespace information
	if pdCoord.register != nil {
		newNamespaces, _, err := pdCoord.register.GetAllNamespaces()
		if err != nil {
			// may not init any yet.
			if err != cluster.ErrKeyNotFound {
				cluster.CoordLog().Infof("load namespace info failed: %v", err)
			}
		} else {
			cluster.CoordLog().Infof("namespace loaded : %v", len(newNamespaces))
			// save to file in case of etcd data disaster
			pdCoord.saveClusterToFile(newNamespaces)
		}
	}

	pdCoord.wg.Add(1)
	go func() {
		defer pdCoord.wg.Done()
		pdCoord.handleDataNodes(monitorChan, true)
	}()
	pdCoord.wg.Add(1)
	go func() {
		defer pdCoord.wg.Done()
		pdCoord.checkNamespaces(monitorChan)
	}()
	pdCoord.wg.Add(1)
	go func() {
		defer pdCoord.wg.Done()
		pdCoord.dpm.DoBalance(monitorChan)
	}()
	pdCoord.wg.Add(1)
	go func() {
		defer pdCoord.wg.Done()
		pdCoord.handleRemovingNodes(monitorChan)
	}()
}

func (pdCoord *PDCoordinator) getCurrentNodes(tags map[string]interface{}) map[string]cluster.NodeInfo {
	pdCoord.nodesMutex.RLock()
	currentNodes := pdCoord.dataNodes
	if len(pdCoord.removingNodes) > 0 || len(tags) > 0 {
		currentNodes = make(map[string]cluster.NodeInfo)
		for nid, n := range pdCoord.dataNodes {
			if _, ok := pdCoord.removingNodes[nid]; ok {
				continue
			}
			filtered := false
			for tag, tagV := range tags {
				if nodeTagV, ok := n.Tags[tag]; !ok {
					filtered = true
					break
				} else {
					switch tag {
					case cluster.DCInfoTag:
						s1, _ := tagV.(string)
						s2, _ := nodeTagV.(string)
						// get only nodes in the same dc
						if s1 != "" && s1 != s2 {
							filtered = true
							break
						}
					}

				}
			}
			if filtered {
				cluster.CoordLog().Infof("node %v is filtered while get nodes based on tags: %v", nid, tags)
				continue
			}
			currentNodes[nid] = n
		}
	}
	pdCoord.nodesMutex.RUnlock()
	return currentNodes
}

func (pdCoord *PDCoordinator) getCurrentNodesWithRemoving() (map[string]cluster.NodeInfo, int64) {
	pdCoord.nodesMutex.RLock()
	currentNodes := pdCoord.dataNodes
	currentNodesEpoch := atomic.LoadInt64(&pdCoord.nodesEpoch)
	pdCoord.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (pdCoord *PDCoordinator) getCurrentLearnerNodes() (map[string]cluster.NodeInfo, int64) {
	pdCoord.nodesMutex.RLock()
	currentNodes := pdCoord.learnerNodes
	currentNodesEpoch := atomic.LoadInt64(&pdCoord.nodesEpoch)
	pdCoord.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (pdCoord *PDCoordinator) hasRemovingNode() bool {
	pdCoord.nodesMutex.RLock()
	num := len(pdCoord.removingNodes)
	pdCoord.nodesMutex.RUnlock()
	return num > 0
}

func (pdCoord *PDCoordinator) getCurrentNodesWithEpoch(tags map[string]interface{}) (map[string]cluster.NodeInfo, int64) {
	pdCoord.nodesMutex.RLock()
	currentNodes := pdCoord.dataNodes
	if len(pdCoord.removingNodes) > 0 || len(tags) > 0 {
		currentNodes = make(map[string]cluster.NodeInfo)
		for nid, n := range pdCoord.dataNodes {
			if _, ok := pdCoord.removingNodes[nid]; ok {
				continue
			}
			filtered := false
			for tag := range tags {
				if _, ok := n.Tags[tag]; !ok {
					filtered = true
					break
				}
			}
			if filtered {
				continue
			}

			currentNodes[nid] = n
		}
	}
	currentNodesEpoch := atomic.LoadInt64(&pdCoord.nodesEpoch)
	pdCoord.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (pdCoord *PDCoordinator) handleDataNodes(monitorChan chan struct{}, isMaster bool) {
	nodesChan := make(chan []cluster.NodeInfo)
	if pdCoord.register != nil {
		go pdCoord.register.WatchDataNodes(nodesChan, monitorChan)
	}
	cluster.CoordLog().Debugf("start watch the nodes.")
	defer func() {
		cluster.CoordLog().Infof("stop watch the nodes.")
	}()
	for {
		select {
		case nodes, ok := <-nodesChan:
			if !ok {
				return
			}
			// check if any node changed.
			cluster.CoordLog().Infof("Current data nodes: %v", len(nodes))
			oldNodes := pdCoord.dataNodes
			oldLearnerNodes := pdCoord.learnerNodes
			newNodes := make(map[string]cluster.NodeInfo)
			newLearnerNodes := make(map[string]cluster.NodeInfo)
			for _, v := range nodes {
				//cluster.CoordLog().Infof("node %v : %v", v.GetID(), v)
				if v.LearnerRole == "" {
					if old, ok := oldLearnerNodes[v.GetID()]; ok {
						cluster.CoordLog().Errorf("old learner role changed from %v to %v, should not allowed", old, v)
						continue
					}
					newNodes[v.GetID()] = v
				} else {
					if old, ok := oldNodes[v.GetID()]; ok {
						cluster.CoordLog().Errorf("old learner role changed from %v to %v, should not allowed", old, v)
						continue
					}
					newLearnerNodes[v.GetID()] = v
				}
			}
			pdCoord.nodesMutex.Lock()
			pdCoord.dataNodes = newNodes
			pdCoord.learnerNodes = newLearnerNodes
			check := false
			for oldID, oldNode := range oldNodes {
				if _, ok := newNodes[oldID]; !ok {
					cluster.CoordLog().Warningf("node failed: %v, %v", oldID, oldNode)
					// if node is missing we need check election immediately.
					check = true
				}
			}
			// failed need be protected by lock so we can avoid contention.
			if check {
				atomic.AddInt64(&pdCoord.nodesEpoch, 1)
			}
			if int32(len(pdCoord.dataNodes)) > atomic.LoadInt32(&pdCoord.stableNodeNum) {
				oldNum := atomic.LoadInt32(&pdCoord.stableNodeNum)
				atomic.StoreInt32(&pdCoord.stableNodeNum, int32(len(pdCoord.dataNodes)))
				cluster.CoordLog().Warningf("stable cluster node number changed from %v to: %v", oldNum, atomic.LoadInt32(&pdCoord.stableNodeNum))
			}
			pdCoord.nodesMutex.Unlock()

			if pdCoord.register == nil {
				continue
			}
			for newID, newNode := range newNodes {
				if _, ok := oldNodes[newID]; !ok {
					cluster.CoordLog().Infof("new node joined: %v, %v", newID, newNode)
					check = true
				}
			}
			if check && isMaster {
				atomic.AddInt64(&pdCoord.nodesEpoch, 1)
				atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
				pdCoord.triggerCheckNamespaces("", 0, time.Millisecond*10)
			}
		}
	}
}

func (pdCoord *PDCoordinator) handleRemovingNodes(monitorChan chan struct{}) {
	cluster.CoordLog().Debugf("start handle the removing nodes.")
	defer func() {
		cluster.CoordLog().Infof("stop handle the removing nodes.")
	}()
	ticker := time.NewTicker(checkRemovingNodeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			anyStateChanged := false
			pdCoord.nodesMutex.RLock()
			removingNodes := make(map[string]string)
			for nid, removeState := range pdCoord.removingNodes {
				removingNodes[nid] = removeState
			}
			pdCoord.nodesMutex.RUnlock()
			// remove state: marked -> pending -> data_transferred -> done
			if len(removingNodes) == 0 {
				continue
			}
			currentNodes := pdCoord.getCurrentNodes(nil)
			nodeNameList := getNodeNameList(currentNodes)

			allNamespaces, _, err := pdCoord.register.GetAllNamespaces()
			if err != nil {
				continue
			}
			for nid := range removingNodes {
				anyPending := false
				cluster.CoordLog().Infof("handle removing node %v ", nid)
				// only check the namespace with one replica left
				// because the doCheckNamespaces will check the others
				// we add a new replica for the removing node

				// to avoid too much migration, we break early if any pending migration found
				for _, namespacePartList := range allNamespaces {
					for _, tmpNsInfo := range namespacePartList {
						namespaceInfo := *(tmpNsInfo.GetCopy())
						if cluster.FindSlice(namespaceInfo.RaftNodes, nid) == -1 {
							continue
						}
						if _, ok := namespaceInfo.Removings[nid]; ok {
							cluster.CoordLog().Infof("namespace %v data on node %v is in removing, waiting", namespaceInfo.GetDesp(), nid)
							anyPending = true
							break
						}
						if anyPending {
							// waiting other pending
							break
						}
						if len(namespaceInfo.GetISR()) <= namespaceInfo.Replica {
							anyPending = true
							// find new catchup and wait isr ready
							removingNodes[nid] = "pending"
							newInfo, err := pdCoord.dpm.addNodeToNamespaceAndWaitReady(monitorChan, &namespaceInfo,
								nodeNameList)
							if err != nil {
								cluster.CoordLog().Infof("namespace %v data on node %v transferred failed, waiting next time", namespaceInfo.GetDesp(), nid)
								break
							} else if newInfo != nil {
								namespaceInfo = *newInfo
							}
							cluster.CoordLog().Infof("namespace %v data on node %v transferred success", namespaceInfo.GetDesp(), nid)
							anyStateChanged = true
						}
						ok, err := IsAllISRFullReady(&namespaceInfo)
						if err != nil || !ok {
							cluster.CoordLog().Infof("namespace %v isr is not full ready: %v", namespaceInfo.GetDesp(), err)
							anyPending = true
							if removingNodes[nid] != "pending" {
								removingNodes[nid] = "pending"
								anyStateChanged = true
							}
							break
						}
						cluster.CoordLog().Infof("namespace %v data on node %v removing", namespaceInfo.GetDesp(), nid)
						coordErr := pdCoord.removeNamespaceFromNode(&namespaceInfo, nid)
						if coordErr != nil {
							anyPending = true
						} else if _, waitingRemove := namespaceInfo.Removings[nid]; waitingRemove {
							anyPending = true
						}
					}
					if !anyPending {
						anyStateChanged = true
						cluster.CoordLog().Infof("node %v data has been transferred, it can be removed from cluster: state: %v", nid, removingNodes[nid])
						if removingNodes[nid] != "data_transferred" && removingNodes[nid] != "done" {
							removingNodes[nid] = "data_transferred"
						} else {
							if removingNodes[nid] == "data_transferred" {
								removingNodes[nid] = "done"
							} else if removingNodes[nid] == "done" {
								pdCoord.nodesMutex.Lock()
								_, ok := pdCoord.dataNodes[nid]
								if !ok {
									delete(removingNodes, nid)
									cluster.CoordLog().Infof("the node %v is removed finally since not alive in cluster", nid)
								}
								pdCoord.nodesMutex.Unlock()
							}
						}
					}
				}
			}

			if anyStateChanged {
				pdCoord.nodesMutex.Lock()
				pdCoord.removingNodes = removingNodes
				pdCoord.nodesMutex.Unlock()
			}
		}
	}
}

func (pdCoord *PDCoordinator) triggerCheckNamespaces(namespace string, part int, delay time.Duration) {
	time.Sleep(delay)

	select {
	case pdCoord.checkNamespaceFailChan <- cluster.NamespaceNameInfo{NamespaceName: namespace, NamespacePartition: part}:
	case <-pdCoord.stopChan:
		return
	case <-time.After(time.Second):
		return
	}
}

// check if partition is enough,
// check if replication is enough
// check any unexpected state.
func (pdCoord *PDCoordinator) checkNamespaces(monitorChan chan struct{}) {
	ticker := time.NewTicker(nsCheckInterval)
	waitingMigrateNamespace := make(map[string]map[int]time.Time)
	defer func() {
		ticker.Stop()
		cluster.CoordLog().Infof("check namespaces quit.")
	}()

	if pdCoord.register == nil {
		return
	}
	lastSaved := time.Now()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			pdCoord.doCheckNamespaces(monitorChan, nil, waitingMigrateNamespace, true)
			if time.Since(lastSaved) > time.Hour*12 {
				allNamespaces, _, err := pdCoord.register.GetAllNamespaces()
				if err == nil {
					lastSaved = time.Now()
					pdCoord.saveClusterToFile(allNamespaces)
				}
			}
		case failedInfo := <-pdCoord.checkNamespaceFailChan:
			pdCoord.doCheckNamespaces(monitorChan, &failedInfo, waitingMigrateNamespace, failedInfo.NamespaceName == "")
		}
	}
}

func (pdCoord *PDCoordinator) doCheckNamespaces(monitorChan chan struct{}, failedInfo *cluster.NamespaceNameInfo,
	waitingMigrateNamespace map[string]map[int]time.Time, fullCheck bool) {

	if !atomic.CompareAndSwapInt32(&pdCoord.doChecking, 0, 1) {
		return
	}
	time.Sleep(time.Millisecond * 10)
	defer atomic.StoreInt32(&pdCoord.doChecking, 0)

	namespaces := []cluster.PartitionMetaInfo{}
	if failedInfo == nil || failedInfo.NamespaceName == "" || failedInfo.NamespacePartition < 0 {
		allNamespaces, _, commonErr := pdCoord.register.GetAllNamespaces()
		if commonErr != nil {
			if commonErr != cluster.ErrKeyNotFound {
				cluster.CoordLog().Infof("scan namespaces failed. %v", commonErr)
				atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
			}
			return
		}
		for n, parts := range allNamespaces {
			if failedInfo != nil && failedInfo.NamespaceName != "" && failedInfo.NamespaceName != n {
				continue
			}
			for _, p := range parts {
				namespaces = append(namespaces, *(p.GetCopy()))
			}
		}
		cluster.CoordLog().Debugf("scan found namespaces: %v", namespaces)
	} else {
		var err error
		cluster.CoordLog().Infof("check single namespace : %v ", failedInfo)
		var t *cluster.PartitionMetaInfo
		t, err = pdCoord.register.GetNamespacePartInfo(failedInfo.NamespaceName, failedInfo.NamespacePartition)
		if err != nil {
			cluster.CoordLog().Infof("get namespace info failed: %v, %v", failedInfo, err)
			atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
			return
		}
		namespaces = append(namespaces, *t)
	}

	// TODO: check partition number for namespace, maybe failed to create
	// some partition when creating namespace.

	currentNodes, currentNodesEpoch := pdCoord.getCurrentNodesWithRemoving()
	cluster.CoordLog().Infof("do check namespaces (%v), current nodes: %v, ...", len(namespaces), len(currentNodes))
	checkOK := true
	fullReady := true
	defer func() {
		if checkOK {
			if fullCheck && fullReady {
				atomic.StoreInt32(&pdCoord.isClusterUnstable, 0)
				pdCoord.doSchemaCheck()
			}
		} else {
			atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
		}
	}()
	for _, nsInfo := range namespaces {
		if currentNodesEpoch != atomic.LoadInt64(&pdCoord.nodesEpoch) {
			cluster.CoordLog().Infof("nodes changed while checking namespaces: %v, %v", currentNodesEpoch, atomic.LoadInt64(&pdCoord.nodesEpoch))
			checkOK = false
			return
		}
		select {
		case <-monitorChan:
			// exiting
			return
		default:
		}

		needMigrate := false
		if len(nsInfo.GetISR()) < nsInfo.Replica {
			cluster.CoordLog().Infof("replicas is not enough for namespace %v, isr is :%v", nsInfo.GetDesp(), nsInfo.GetISR())
			needMigrate = true
			checkOK = false
		}

		aliveCount := 0
		for _, replica := range nsInfo.GetISR() {
			if _, ok := currentNodes[replica]; !ok {
				cluster.CoordLog().Warningf("namespace %v isr node %v is lost.", nsInfo.GetDesp(), replica)
				needMigrate = true
				checkOK = false
			} else {
				aliveCount++
			}
		}
		if currentNodesEpoch != atomic.LoadInt64(&pdCoord.nodesEpoch) {
			cluster.CoordLog().Infof("nodes changed while checking namespaces: %v, %v", currentNodesEpoch, atomic.LoadInt64(&pdCoord.nodesEpoch))
			checkOK = false
			return
		}
		if int32(len(currentNodes)) <= atomic.LoadInt32(&pdCoord.stableNodeNum)/2 {
			checkOK = false
			cluster.CoordLog().Infof("nodes not enough while checking: %v, stable need: %v", currentNodes, atomic.LoadInt32(&pdCoord.stableNodeNum))
			return
		}
		_, regErr := pdCoord.register.GetRemoteNamespaceReplicaInfo(nsInfo.Name, nsInfo.Partition)
		if regErr != nil {
			cluster.CoordLog().Warningf("get remote namespace %v failed:%v, etcd may be unreachable.",
				nsInfo.GetDesp(), regErr)
			atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
			checkOK = false
			continue
		}

		partitions, ok := waitingMigrateNamespace[nsInfo.Name]
		if !ok {
			partitions = make(map[int]time.Time)
			waitingMigrateNamespace[nsInfo.Name] = partitions
		}

		// handle removing should before migrate since the migrate may be blocked by the
		// removing node.
		if atomic.LoadInt32(&pdCoord.balanceWaiting) == 0 && len(nsInfo.Removings) > 0 {
			pdCoord.removeNamespaceFromRemovings(&nsInfo)
		}

		if needMigrate && pdCoord.AutoBalanceEnabled() {
			if _, ok := partitions[nsInfo.Partition]; !ok {
				partitions[nsInfo.Partition] = time.Now()
				// migrate next time
				continue
			}

			if atomic.LoadInt32(&pdCoord.isUpgrading) == 1 {
				cluster.CoordLog().Infof("wait checking namespaces since the cluster is upgrading")
				continue
			}
			failedTime := partitions[nsInfo.Partition]
			if failedTime.Before(time.Now().Add(-1 * waitMigrateInterval)) {
				aliveNodes, aliveEpoch := pdCoord.getCurrentNodesWithEpoch(nsInfo.Tags)
				if aliveEpoch != currentNodesEpoch {
					go pdCoord.triggerCheckNamespaces(nsInfo.Name, nsInfo.Partition, time.Second)
					continue
				}
				cluster.CoordLog().Infof("begin migrate the namespace :%v", nsInfo.GetDesp())
				if coordErr := pdCoord.handleNamespaceMigrate(&nsInfo, aliveNodes, aliveEpoch); coordErr != nil {
					atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
					continue
				} else {
					delete(partitions, nsInfo.Partition)
					// migrate only one at once to reduce the migrate traffic
					atomic.StoreInt32(&pdCoord.isClusterUnstable, 1)
					for _, parts := range waitingMigrateNamespace {
						for pid := range parts {
							parts[pid].Add(time.Second)
						}
					}
				}
			} else {
				cluster.CoordLog().Infof("waiting migrate the namespace :%v since time: %v", nsInfo.GetDesp(), partitions[nsInfo.Partition])
			}
		} else {
			delete(partitions, nsInfo.Partition)
			if ok, err := IsAllISRFullReady(&nsInfo); err != nil || !ok {
				fullReady = false
				cluster.CoordLog().Infof("namespace %v isr is not full ready", nsInfo.GetDesp())
				continue
			}
		}

		if atomic.LoadInt32(&pdCoord.balanceWaiting) == 0 {
			if aliveCount > nsInfo.Replica && !needMigrate {
				canRemove := true
				if pdCoord.hasRemovingNode() {
					canRemove = false
				}
				if canRemove {
					ok, err := IsAllISRFullReady(&nsInfo)
					if err != nil {
						cluster.CoordLog().Infof("namespace %v isr is not full ready: %v", nsInfo.GetDesp(), err.Error())
					}
					if !ok {
						canRemove = false
					}
				}
				//remove the unwanted node in isr
				cluster.CoordLog().Infof("namespace %v isr %v is more than replicator: %v, %v", nsInfo.GetDesp(),
					nsInfo.RaftNodes, aliveCount, nsInfo.Replica)
				if canRemove {
					removeNode := pdCoord.dpm.decideUnwantedRaftNode(&nsInfo, currentNodes)
					if removeNode != "" {
						coordErr := pdCoord.removeNamespaceFromNode(&nsInfo, removeNode)
						if coordErr == nil {
							cluster.CoordLog().Infof("node %v removed by plan from namespace : %v", removeNode, nsInfo)
						}
					}
				}
			}
		}
	}

}

func (pdCoord *PDCoordinator) handleNamespaceMigrate(origNSInfo *cluster.PartitionMetaInfo,
	currentNodes map[string]cluster.NodeInfo, currentNodesEpoch int64) *cluster.CoordErr {
	if currentNodesEpoch != atomic.LoadInt64(&pdCoord.nodesEpoch) {
		return cluster.ErrClusterChanged
	}
	if len(origNSInfo.Removings) > 0 {
		cluster.CoordLog().Infof("namespace: %v still waiting removing node: %v", origNSInfo.GetDesp(), origNSInfo.Removings)
		return ErrNamespaceMigrateWaiting
	}
	isrChanged := false
	aliveReplicas := 0
	nsInfo := origNSInfo.GetCopy()
	for _, replica := range nsInfo.RaftNodes {
		if _, ok := currentNodes[replica]; ok {
			aliveReplicas++
			// if the other alive replica is not synced, it means the raft group may be unstable,
			// so we should not remove any node until other replicas became stable to avoid a broken raft group.
			synced, err := IsRaftNodeSynced(nsInfo, replica)
			if err != nil || !synced {
				cluster.CoordLog().Infof("namespace: %v replica %v is not synced while removing node, need wait", nsInfo.GetDesp(), replica)
				return ErrNamespaceMigrateWaiting
			}
		} else {
			cluster.CoordLog().Infof("failed raft node %v for namespace: %v",
				replica, nsInfo.GetDesp())
			if nsInfo.Removings == nil {
				nsInfo.Removings = make(map[string]cluster.RemovingInfo)
			}
			if _, ok := nsInfo.Removings[replica]; !ok {
				if len(nsInfo.Removings) == 0 && len(nsInfo.GetISR())-1 > nsInfo.Replica/2 {
					cluster.CoordLog().Infof("mark removing for failed raft node %v for namespace: %v, isr: %v",
						replica, nsInfo.GetDesp(), nsInfo.GetISR())
					nsInfo.Removings[replica] = cluster.RemovingInfo{RemoveTime: time.Now().UnixNano(),
						RemoveReplicaID: nsInfo.RaftIDs[replica]}
					isrChanged = true
				}
			}
		}
	}

	// avoid removing any node if current alive replicas is not enough
	if len(nsInfo.Removings) > 0 && aliveReplicas <= nsInfo.Replica/2 {
		cluster.CoordLog().Infof("namespace: %v alive replica %v is not enough while removing node", nsInfo.GetDesp(), aliveReplicas)
		return ErrNamespaceMigrateWaiting
	}

	if len(currentNodes) < nsInfo.Replica && len(nsInfo.Removings) > 0 {
		cluster.CoordLog().Warningf("no enough alive nodes %v for namespace %v replica: %v",
			len(currentNodes), nsInfo.GetDesp(), nsInfo.Replica)
		return ErrNodeUnavailable
	}

	if len(nsInfo.Removings) == 0 {
		ok, err := IsAllISRFullReady(nsInfo)
		if err != nil || !ok {
			cluster.CoordLog().Infof("namespace: %v isr not full ready while add new raft node", nsInfo.GetDesp())
		} else {
			for i := aliveReplicas; i < nsInfo.Replica; i++ {
				n, err := pdCoord.dpm.allocNodeForNamespace(nsInfo, currentNodes)
				if err != nil {
					cluster.CoordLog().Infof("failed to get a new raft node for namespace: %v", nsInfo.GetDesp())
				} else {
					nsInfo.MaxRaftID++
					nsInfo.RaftIDs[n.GetID()] = uint64(nsInfo.MaxRaftID)
					nsInfo.RaftNodes = append(nsInfo.RaftNodes, n.GetID())
					isrChanged = true
				}
				// add raft node one by one to avoid 2 un-synced raft nodes
				break
			}
		}
	}

	// to avoid (1,2) become (1), make sure we always have quorum replicas in raft.
	// So we should add new replica before we continue remove replica.
	// However, if there is a failed node in (1, 2) replicas, we can not add new because we can not have quorum voters in raft.
	// In this way, we need wait failed node restart or we manual force recovery with standalone node.

	// TODO: Consider the case while we force init a standalone raft which has only 1 replica, but the
	// replicator desired is 4 or more. We will add one node and the isr is only 2 (not Quorum) and
	// we should allow the standalone raft group to grow as needed.
	if isrChanged && nsInfo.IsISRQuorum() {
		if len(nsInfo.Removings) > 1 {
			cluster.CoordLog().Infof("namespace should not have two removing nodes: %v", nsInfo)
			return cluster.ErrNamespaceConfInvalid
		}
		err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
			&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
		if err != nil {
			cluster.CoordLog().Infof("update namespace replica info failed: %v", err.Error())
			return cluster.ErrRegisterServiceUnstable
		}
		cluster.CoordLog().Infof("namespace %v migrate to replicas : %v", nsInfo.GetDesp(), nsInfo.RaftNodes)
		*origNSInfo = *nsInfo
	} else {
		cluster.CoordLog().Infof("namespace %v waiting migrate : %v", nsInfo.GetDesp(), nsInfo)
		return ErrNamespaceMigrateWaiting
	}
	return nil
}

// make sure check raft synced before add new node to isr to avoid 2 un-synced raft nodes
func (pdCoord *PDCoordinator) addNamespaceToNode(origNSInfo *cluster.PartitionMetaInfo, nid string) *cluster.CoordErr {
	if len(origNSInfo.Removings) > 0 {
		// we do not add new node until the removing node is actually removed
		// because we need avoid too much failed node in the cluster,
		// if the new added node failed to join cluster, we got 2 nodes failed in 4 cluster
		// which maybe cause the cluster is hard to became 4 nodes cluster since the removing
		// node maybe can not restart in short time
		return cluster.ErrNamespaceWaitingSync
	}
	nsInfo := origNSInfo.GetCopy()
	nsInfo.RaftNodes = append(nsInfo.RaftNodes, nid)
	if !pdCoord.dpm.checkNamespaceNodeConflict(nsInfo) {
		return ErrNamespaceNodeConflict
	}
	nsInfo.MaxRaftID++
	nsInfo.RaftIDs[nid] = uint64(nsInfo.MaxRaftID)

	err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("add namespace replica info failed: %v", err.Error())
		return &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	} else {
		cluster.CoordLog().Infof("namespace %v replica : %v added", nsInfo.GetDesp(), nid)
		*origNSInfo = *nsInfo
	}
	return nil
}

// should avoid mark as removing if there are not enough alive nodes for replicator.
func (pdCoord *PDCoordinator) removeNamespaceFromNode(origNSInfo *cluster.PartitionMetaInfo, nid string) *cluster.CoordErr {
	_, ok := origNSInfo.Removings[nid]
	if ok {
		// already removed
		return nil
	}
	if _, ok := origNSInfo.RaftIDs[nid]; !ok {
		return ErrNamespaceRaftIDNotFound
	}
	if !origNSInfo.IsISRQuorum() {
		return ErrNamespaceReplicaNotEnough
	}
	if origNSInfo.Removings == nil {
		origNSInfo.Removings = make(map[string]cluster.RemovingInfo)
	}
	// at most one node removing, wait removing node removed from removings
	if len(origNSInfo.Removings) > 0 {
		return ErrNamespaceMigrateWaiting
	}
	nsInfo := origNSInfo.GetCopy()
	cluster.CoordLog().Infof("namespace %v: mark replica removing , current isr: %v", nsInfo.GetDesp(),
		nsInfo.GetISR())
	nsInfo.Removings[nid] = cluster.RemovingInfo{RemoveTime: time.Now().UnixNano(), RemoveReplicaID: nsInfo.RaftIDs[nid]}
	if !nsInfo.IsISRQuorum() || len(nsInfo.Removings) > 1 {
		return ErrNamespaceReplicaNotEnough
	}
	err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("update namespace replica info failed: %v", err.Error())
		return &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	} else {
		cluster.CoordLog().Infof("namespace %v: mark replica removing from node:%v, current isr: %v", nsInfo.GetDesp(),
			nsInfo.Removings[nid], nsInfo.GetISR())
		*origNSInfo = *nsInfo
	}
	return nil
}

func (pdCoord *PDCoordinator) removeNamespaceFromRemovings(origNSInfo *cluster.PartitionMetaInfo) {
	// make sure all the current members are notified the newest cluster info and
	// have the raft synced
	nsInfo := origNSInfo.GetCopy()
	changed := false
	for nid, rinfo := range nsInfo.Removings {
		if rinfo.RemoveTime == 0 {
			// it may be zero while pd leader changed while node removing by old pd leader
			rinfo.RemoveTime = time.Now().UnixNano()
			continue
		}
		if time.Now().UnixNano()-rinfo.RemoveTime < waitRemoveRemovingNodeInterval.Nanoseconds() {
			continue
		}
		inRaft, err := IsRaftNodeJoined(nsInfo, nid)
		if inRaft || err != nil {
			continue
		}
		nodes := make([]string, 0, len(nsInfo.RaftNodes))
		for _, replica := range nsInfo.RaftNodes {
			if nid == replica {
				continue
			}
			nodes = append(nodes, replica)
		}
		if len(nodes) < 1 {
			cluster.CoordLog().Infof("single replica can not be removed from namespace %v ", nsInfo.GetDesp())
			continue
		}
		nsInfo.RaftNodes = nodes
		delete(nsInfo.RaftIDs, nid)
		delete(nsInfo.Removings, nid)
		changed = true
		cluster.CoordLog().Infof("namespace %v replica removed from removing node:%v, %v", nsInfo.GetDesp(), nid, rinfo)
	}
	if changed && nsInfo.IsISRQuorum() {
		err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
			&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
		if err != nil {
			cluster.CoordLog().Infof("update namespace replica info failed: %v", err.Error())
		} else {
			*origNSInfo = *nsInfo
		}
	}
}

package datanode_coord

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/common"
	node "github.com/youzan/ZanRedisDB/node"
)

func (dc *DataCoordinator) loadLocalNamespaceForLearners() error {
	if dc.localNSMgr == nil {
		cluster.CoordLog().Infof("no namespace manager")
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
			if localNamespace != nil {
				// already loaded
				joinErr := dc.ensureJoinNamespaceGroupForLearner(nsInfo, localNamespace, false)
				if joinErr != nil && joinErr != cluster.ErrNamespaceConfInvalid {
					// we ensure join group as order for partitions
					break
				}
				if cluster.CoordLog().Level() >= common.LOG_DETAIL {
					cluster.CoordLog().Debugf("%v namespace %v already loaded", dc.GetMyID(), nsInfo.GetDesp())
				}
				continue
			}
			if !dc.isInLearnerGroup(nsInfo, nil) {
				continue
			}
			cluster.CoordLog().Infof("loading namespace as learner: %v", nsInfo.GetDesp())
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
			joinErr := dc.ensureJoinNamespaceGroupForLearner(nsInfo, localNamespace, true)
			if joinErr != nil && joinErr != cluster.ErrNamespaceConfInvalid {
				// we ensure join group as order for partitions
				break
			}
		}
	}
	return nil
}

func (dc *DataCoordinator) ensureJoinNamespaceGroupForLearner(nsInfo cluster.PartitionMetaInfo,
	localNamespace *node.NamespaceNode, firstLoad bool) *cluster.CoordErr {

	lrns := nsInfo.LearnerNodes[dc.learnerRole]
	if len(lrns) > 0 && lrns[0] == dc.GetMyID() {
		localNamespace.SwitchForLearnerLeader(true)
	} else {
		localNamespace.SwitchForLearnerLeader(false)
	}

	dyConf := &node.NamespaceDynamicConf{}
	localNamespace.SetDynamicInfo(*dyConf)
	if localNamespace.IsDataNeedFix() {
		// clean local data
	}
	localNamespace.SetDataFixState(false)
	if !dc.isInLearnerGroup(nsInfo, localNamespace) {
		return nil
	}
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
		mems := localNamespace.GetLearners()
		alreadyJoined := false
		for _, m := range mems {
			if m.NodeID == dc.GetMyRegID() &&
				m.GroupName == nsInfo.GetDesp() &&
				m.ID == raftID {
				alreadyJoined = true
				break
			}
		}
		if alreadyJoined {
			joinErr = nil
			break
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
			}
			time.Sleep(time.Millisecond * 100)
			if _, ok := requestJoined[remote]; !ok {
				err := dc.requestJoinNamespaceGroup(raftID, &nsInfo, localNamespace, remote, true)
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

func (dc *DataCoordinator) isInLearnerGroup(nsInfo cluster.PartitionMetaInfo, localNamespace *node.NamespaceNode) bool {
	// removing node can stop local raft only when all the other members
	// are notified to remove this node
	// Mostly, the remove node proposal will handle the raft node stop, however
	// there are some situations to be checked here.
	inMeta := false
	for _, nid := range nsInfo.LearnerNodes[dc.learnerRole] {
		if nid == dc.GetMyID() {
			if localNamespace == nil {
				inMeta = true
				break
			}
			replicaID := nsInfo.RaftIDs[nid]
			if replicaID == localNamespace.GetRaftID() {
				inMeta = true
				break
			}
		}
	}
	return inMeta
}

func (dc *DataCoordinator) checkForUnsyncedLogSyncers() {
	ticker := time.NewTicker(CheckUnsyncedLearnerInterval)
	cluster.CoordLog().Infof("%v begin to check for unsynced log syncers", dc.GetMyID())
	defer cluster.CoordLog().Infof("%v check for unsynced log syncers quit", dc.GetMyID())
	defer dc.wg.Done()

	doWork := func() {
		if atomic.LoadInt32(&dc.stopping) == 1 {
			return
		}
		// try load local namespace if any namespace raft group changed
		err := dc.loadLocalNamespaceForLearners()
		if err != nil {
			cluster.CoordLog().Infof("%v load local error : %v", dc.GetMyID(), err.Error())
		}

		// check local namespaces with cluster to remove the unneed data
		tmpChecks := dc.localNSMgr.GetNamespaces()
		for name, localNamespace := range tmpChecks {
			namespace, pid := common.GetNamespaceAndPartition(name)
			if namespace == "" {
				cluster.CoordLog().Warningf("namespace invalid: %v", name)
				continue
			}
			nsInfo, err := dc.register.GetNamespacePartInfo(namespace, pid)
			if err != nil {
				cluster.CoordLog().Infof("got namespace %v meta failed: %v", name, err)
				if err == cluster.ErrKeyNotFound {
					dc.forceRemoveLocalNamespace(localNamespace)
				} else {
					dc.tryCheckNamespacesIn(time.Second * 5)
				}
				continue
			}

			if !dc.isInLearnerGroup(*nsInfo, localNamespace) {
				cluster.CoordLog().Infof("namespace %v removed since not in learner group", name, nsInfo.LearnerNodes)
				dc.forceRemoveLocalNamespace(localNamespace)
				continue
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
		}
	}
}

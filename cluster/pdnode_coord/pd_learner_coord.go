package pdnode_coord

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/cluster"
)

const (
	pdRegisterKVLearnerStartPrefix = "placedriver:learner:need_start_learner:"
)

func (pdCoord *PDCoordinator) checkNamespacesForLearner(monitorChan chan struct{}) {
	ticker := time.NewTicker(nsCheckLearnerInterval)
	defer func() {
		ticker.Stop()
		cluster.CoordLog().Infof("check namespaces quit.")
	}()

	if pdCoord.register == nil {
		return
	}
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			pdCoord.doCheckNamespacesForLearner(monitorChan)
		}
	}
}

func (pdCoord *PDCoordinator) cleanAllLearners(ns string) error {
	oldMeta, err := pdCoord.register.GetNamespaceMetaInfo(ns)
	if err != nil {
		cluster.CoordLog().Infof("get namespace key %v failed :%v", ns, err)
		return err
	}
	for i := 0; i < oldMeta.PartitionNum; i++ {
		origNSInfo, err := pdCoord.register.GetNamespacePartInfo(ns, i)
		if err != nil {
			cluster.CoordLog().Infof("get namespace %v info failed :%v", ns, err)
			continue
		}
		err = pdCoord.removeNsAllLearners(origNSInfo)
		if err != nil {
			cluster.CoordLog().Infof("namespace %v-%v remove learner failed :%v", ns, i, err.Error())
			return err
		}
	}
	return nil
}

// use this to switch start or stop learner without restart the node
func (pdCoord *PDCoordinator) SwitchStartLearner(enable bool) error {
	if pdCoord.learnerRole == "" {
		return nil
	}
	value := "false"
	if enable {
		value = "true"
	}
	return pdCoord.register.SaveKV(pdRegisterKVLearnerStartPrefix+pdCoord.learnerRole, value)
}

func (pdCoord *PDCoordinator) GetLearnerRunningState() (bool, error) {
	v, err := pdCoord.register.GetKV(pdRegisterKVLearnerStartPrefix + pdCoord.learnerRole)
	// we use not equal false to make sure the false running is always determined
	return v != "false", err
}

func (pdCoord *PDCoordinator) doCheckNamespacesForLearner(monitorChan chan struct{}) {
	needLearner, err := pdCoord.register.GetKV(pdRegisterKVLearnerStartPrefix + pdCoord.learnerRole)
	if err != nil {
		cluster.CoordLog().Infof("learner start state (should be one of true/false) read error: %v", err.Error())
		return
	}
	namespaces := []cluster.PartitionMetaInfo{}
	allNamespaces, _, commonErr := pdCoord.register.GetAllNamespaces()
	if commonErr != nil {
		cluster.CoordLog().Infof("scan namespaces failed. %v", commonErr)
		return
	}
	if needLearner == "false" {
		// remove learners from meta info
		// to make sure the learner node is stopped, you should
		// query the api on the learner node.
		for ns, _ := range allNamespaces {
			err = pdCoord.cleanAllLearners(ns)
			if err != nil {
				cluster.CoordLog().Infof("clean namespace %s learner failed. %v", ns, err.Error())
				return
			}
		}
		return
	}
	if needLearner != "true" {
		cluster.CoordLog().Infof("unexpected learner start state (should be one of true/false) : %v", needLearner)
		return
	}
	for _, parts := range allNamespaces {
		for _, p := range parts {
			namespaces = append(namespaces, *(p.GetCopy()))
		}
	}
	cluster.CoordLog().Debugf("scan found namespaces: %v", namespaces)

	currentNodes, currentNodesEpoch := pdCoord.getCurrentLearnerNodes()
	cluster.CoordLog().Debugf("do check namespaces (%v), current learner nodes: %v, ...", len(namespaces), len(currentNodes))
	// filter my learner role
	learnerNodes := make([]cluster.NodeInfo, 0)
	for _, n := range currentNodes {
		if n.LearnerRole != pdCoord.learnerRole {
			continue
		}
		learnerNodes = append(learnerNodes, n)
	}
	for _, nsInfo := range namespaces {
		if currentNodesEpoch != atomic.LoadInt64(&pdCoord.nodesEpoch) {
			cluster.CoordLog().Infof("nodes changed while checking namespaces: %v, %v", currentNodesEpoch, atomic.LoadInt64(&pdCoord.nodesEpoch))
			return
		}
		select {
		case <-monitorChan:
			// exiting
			return
		default:
		}

		if len(nsInfo.GetISR()) <= nsInfo.Replica/2 {
			// wait enough isr for leader election before we add learner
			continue
		}
		// filter some namespace
		if _, ok := pdCoord.filterNamespaces[nsInfo.Name]; ok {
			// need clean learner for old configure (new configure add new filter namespace after
			// old learner already added)
			pdCoord.removeNsAllLearners(&nsInfo)
			continue
		}
		// check current learner node alive
		newMaster := ""
		learnerIDs := nsInfo.LearnerNodes[pdCoord.learnerRole]

		for _, nid := range learnerIDs {
			if _, ok := currentNodes[nid]; !ok {
				cluster.CoordLog().Infof("namespace %v learner node %v is lost.", nsInfo.GetDesp(), nid)
			} else if newMaster == "" {
				newMaster = nid
			}
		}
		if newMaster != "" && newMaster != learnerIDs[0] {
			// update leader learner
			pdCoord.updateNsLearnerLeader(&nsInfo, newMaster)
		}

		// add new learner for new learner node
		for _, node := range learnerNodes {
			found := false
			for _, nid := range learnerIDs {
				if nid == node.GetID() {
					found = true
					break
				}
			}
			if !found {
				pdCoord.addNsLearnerToNode(&nsInfo, node.GetID())
			}
		}
	}
}

func (pdCoord *PDCoordinator) updateNsLearnerLeader(origNSInfo *cluster.PartitionMetaInfo, nid string) *cluster.CoordErr {
	nsInfo := origNSInfo.GetCopy()
	role := pdCoord.learnerRole
	learnerIDs := nsInfo.LearnerNodes[role]
	found := false
	for index, id := range learnerIDs {
		if id == nid {
			found = true
			learnerIDs[0], learnerIDs[index] = learnerIDs[index], learnerIDs[0]
			break
		}
	}
	if !found {
		return nil
	}
	if nsInfo.LearnerNodes == nil {
		nsInfo.LearnerNodes = make(map[string][]string)
	}
	nsInfo.LearnerNodes[role] = learnerIDs
	err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("namespace %v update learner info failed: %v", nsInfo.GetDesp(), err.Error())
		return &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	} else {
		cluster.CoordLog().Infof("namespace %v learner changed to %v ", nsInfo.GetDesp(), learnerIDs)
		*origNSInfo = *nsInfo
	}
	return nil
}

func (pdCoord *PDCoordinator) addNsLearnerToNode(origNSInfo *cluster.PartitionMetaInfo, nid string) *cluster.CoordErr {
	nsInfo := origNSInfo.GetCopy()
	role := pdCoord.learnerRole
	learnerIDs := nsInfo.LearnerNodes[role]
	for _, id := range learnerIDs {
		if id == nid {
			cluster.CoordLog().Infof("namespace %v learner %v node %v  already exist", nsInfo.GetDesp(), role, nid)
			return nil
		}
	}
	if nsInfo.LearnerNodes == nil {
		nsInfo.LearnerNodes = make(map[string][]string)
	}
	nsInfo.LearnerNodes[role] = append(nsInfo.LearnerNodes[role], nid)
	nsInfo.MaxRaftID++
	nsInfo.RaftIDs[nid] = uint64(nsInfo.MaxRaftID)

	err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("namespace %v add learner info failed: %v", nsInfo.GetDesp(), err.Error())
		return &cluster.CoordErr{ErrMsg: err.Error(), ErrCode: cluster.RpcNoErr, ErrType: cluster.CoordRegisterErr}
	} else {
		cluster.CoordLog().Infof("namespace %v learner role %v: %v added", nsInfo.GetDesp(), role, nid)
		*origNSInfo = *nsInfo
	}
	return nil
}

// remove learner should be manual since learner is not expected to change too often
func (pdCoord *PDCoordinator) removeNsLearnerFromNode(ns string, pid int, nid string, checkNode bool) error {
	origNSInfo, err := pdCoord.register.GetNamespacePartInfo(ns, pid)
	if err != nil {
		return err
	}

	nsInfo := origNSInfo.GetCopy()
	if checkNode {
		currentNodes, _ := pdCoord.getCurrentLearnerNodes()
		if _, ok := currentNodes[nid]; ok {
			cluster.CoordLog().Infof("namespace %v: mark learner node %v removing before stopped", nsInfo.GetDesp(), nid)
			return errors.New("removing learner node should be stopped first")
		}
	}
	role := pdCoord.learnerRole
	cluster.CoordLog().Infof("namespace %v: mark learner role %v node %v removing , current : %v", nsInfo.GetDesp(), role, nid,
		nsInfo.LearnerNodes)

	if nsInfo.LearnerNodes == nil {
		nsInfo.LearnerNodes = make(map[string][]string)
	}
	old := nsInfo.LearnerNodes[role]
	newLrns := make([]string, 0, len(old))
	for _, oid := range old {
		if oid == nid {
			continue
		}
		newLrns = append(newLrns, oid)
	}
	if len(old) == len(newLrns) {
		return errors.New("remove node id is not in learners")
	}
	nsInfo.LearnerNodes[role] = newLrns
	delete(nsInfo.RaftIDs, nid)

	err = pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("update namespace %v replica info failed: %v", nsInfo.GetDesp(), err.Error())
		return err
	} else {
		cluster.CoordLog().Infof("namespace %v: mark learner role %v removing from node:%v done", nsInfo.GetDesp(),
			role, nid)
		*origNSInfo = *nsInfo
	}
	return nil
}

func (pdCoord *PDCoordinator) removeNsAllLearners(origNSInfo *cluster.PartitionMetaInfo) error {
	nsInfo := origNSInfo.GetCopy()
	role := pdCoord.learnerRole
	if nsInfo.LearnerNodes == nil {
		nsInfo.LearnerNodes = make(map[string][]string)
	}
	old := nsInfo.LearnerNodes[role]
	if len(old) == 0 {
		return nil
	}
	cluster.CoordLog().Infof("namespace %v: removing all learner role %v nodes %v",
		nsInfo.GetDesp(), role, old)
	for _, nid := range old {
		delete(nsInfo.RaftIDs, nid)
	}
	nsInfo.LearnerNodes[role] = make([]string, 0)

	err := pdCoord.register.UpdateNamespacePartReplicaInfo(nsInfo.Name, nsInfo.Partition,
		&nsInfo.PartitionReplicaInfo, nsInfo.PartitionReplicaInfo.Epoch())
	if err != nil {
		cluster.CoordLog().Infof("update namespace %v replica info failed: %v", nsInfo.GetDesp(), err.Error())
		return err
	} else {
		cluster.CoordLog().Infof("namespace %v: removed all learner role %v node:%v", nsInfo.GetDesp(),
			role, old)
		*origNSInfo = *nsInfo
	}
	return nil
}

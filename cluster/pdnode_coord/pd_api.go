package pdnode_coord

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/youzan/ZanRedisDB/cluster"
	"github.com/youzan/ZanRedisDB/common"
)

// some API for outside
func (pdCoord *PDCoordinator) IsClusterStable() bool {
	return atomic.LoadInt32(&pdCoord.isClusterUnstable) == 0 &&
		atomic.LoadInt32(&pdCoord.isUpgrading) == 0
}

func (pdCoord *PDCoordinator) IsMineLeader() bool {
	return pdCoord.leaderNode.GetID() == pdCoord.myNode.GetID()
}

func (pdCoord *PDCoordinator) GetAllPDNodes() ([]cluster.NodeInfo, error) {
	return pdCoord.register.GetAllPDNodes()
}

func (pdCoord *PDCoordinator) GetPDLeader() cluster.NodeInfo {
	return pdCoord.leaderNode
}

func (pdCoord *PDCoordinator) GetAllDataNodes() (map[string]cluster.NodeInfo, int64) {
	return pdCoord.getCurrentNodesWithRemoving()
}

// may return old cached data if register not available
func (pdCoord *PDCoordinator) GetAllNamespaces() (map[string]map[int]cluster.PartitionMetaInfo, int64, error) {
	ns, epoch, err := pdCoord.register.GetAllNamespaces()
	return ns, int64(epoch), err
}

func (pdCoord *PDCoordinator) SwitchAutoBalance(enable bool) {
	if enable {
		atomic.StoreInt32(&pdCoord.autoBalance, 1)
		cluster.CoordLog().Infof("balance enabled")
	} else {
		atomic.StoreInt32(&pdCoord.autoBalance, 0)
		cluster.CoordLog().Infof("balance disabled")
	}
}

func (pdCoord *PDCoordinator) SetClusterStableNodeNum(num int) error {
	if int32(num) > atomic.LoadInt32(&pdCoord.stableNodeNum) {
		return errors.New("cluster stable node number can not be increased by manunal, only decrease allowed")
	}
	atomic.StoreInt32(&pdCoord.stableNodeNum, int32(num))
	return nil
}

func (pdCoord *PDCoordinator) SetClusterUpgradeState(upgrading bool) error {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	if upgrading {
		if !atomic.CompareAndSwapInt32(&pdCoord.isUpgrading, 0, 1) {
			cluster.CoordLog().Infof("the cluster state is already upgrading")
			return nil
		}
		cluster.CoordLog().Infof("the cluster state has been changed to upgrading")
	} else {
		if !atomic.CompareAndSwapInt32(&pdCoord.isUpgrading, 1, 0) {
			return nil
		}
		cluster.CoordLog().Infof("the cluster state has been changed to normal")
		pdCoord.triggerCheckNamespaces("", 0, time.Second)
	}
	return nil
}

func (pdCoord *PDCoordinator) MarkNodeAsRemoving(nid string) error {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	cluster.CoordLog().Infof("try mark node %v as removed", nid)
	pdCoord.nodesMutex.Lock()
	newRemovingNodes := make(map[string]string)
	if _, ok := pdCoord.removingNodes[nid]; ok {
		cluster.CoordLog().Infof("already mark as removing")
	} else {
		newRemovingNodes[nid] = "marked"
		for id, removeState := range pdCoord.removingNodes {
			newRemovingNodes[id] = removeState
		}
		pdCoord.removingNodes = newRemovingNodes
	}
	pdCoord.nodesMutex.Unlock()
	return nil
}

func (pdCoord *PDCoordinator) DeleteNamespace(ns string, partition string) error {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	begin := time.Now()
	for !atomic.CompareAndSwapInt32(&pdCoord.doChecking, 0, 1) {
		cluster.CoordLog().Infof("delete %v waiting check namespace finish", ns)
		time.Sleep(time.Millisecond * 200)
		if time.Since(begin) > time.Second*5 {
			return ErrClusterUnstable
		}
	}
	defer atomic.StoreInt32(&pdCoord.doChecking, 0)
	cluster.CoordLog().Infof("delete namespace: %v, with partition: %v", ns, partition)
	if ok, err := pdCoord.register.IsExistNamespace(ns); !ok {
		cluster.CoordLog().Infof("no namespace : %v", err)
		return cluster.ErrKeyNotFound
	}

	if partition == "**" {
		// delete all
		meta, err := pdCoord.register.GetNamespaceMetaInfo(ns)
		if err != nil {
			cluster.CoordLog().Infof("failed to get meta for namespace: %v", err)
		}
		for pid := 0; pid < meta.PartitionNum; pid++ {
			err := pdCoord.deleteNamespacePartition(ns, pid)
			if err != nil {
				cluster.CoordLog().Infof("failed to delete namespace partition %v for namespace: %v, err:%v", pid, ns, err)
			}
		}
		err = pdCoord.register.DeleteWholeNamespace(ns)
		if err != nil {
			cluster.CoordLog().Infof("failed to delete whole namespace: %v : %v", ns, err)
		}
	}
	return nil
}

func (pdCoord *PDCoordinator) deleteNamespacePartition(namespace string, pid int) error {
	commonErr := pdCoord.register.DeleteNamespacePart(namespace, pid)
	if commonErr != nil {
		cluster.CoordLog().Infof("failed to delete the namespace info : %v", commonErr)
		return commonErr
	}
	return nil
}

func (pdCoord *PDCoordinator) ChangeNamespaceMetaParam(namespace string, newReplicator int,
	optimizeFsync string, snapCount int) error {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while create namespace")
		return ErrNotLeader
	}

	if !common.IsValidNamespaceName(namespace) {
		return errors.New("invalid namespace name")
	}

	if newReplicator > 5 {
		return errors.New("max replicator allowed exceed")
	}

	var meta cluster.NamespaceMetaInfo
	if ok, _ := pdCoord.register.IsExistNamespace(namespace); !ok {
		cluster.CoordLog().Infof("namespace not exist %v :%v", namespace)
		return cluster.ErrNamespaceNotCreated.ToErrorType()
	} else {
		oldMeta, err := pdCoord.register.GetNamespaceMetaInfo(namespace)
		if err != nil {
			cluster.CoordLog().Infof("get namespace key %v failed :%v", namespace, err)
			return err
		}
		currentNodes := pdCoord.getCurrentNodes(oldMeta.Tags)
		meta = oldMeta
		if newReplicator > 0 {
			meta.Replica = newReplicator
		}
		if snapCount > 0 {
			meta.SnapCount = snapCount
		}
		if optimizeFsync == "true" {
			meta.OptimizedFsync = true
		} else if optimizeFsync == "false" {
			meta.OptimizedFsync = false
		}
		err = pdCoord.updateNamespaceMeta(currentNodes, namespace, &meta)
		if err != nil {
			return err
		}
		pdCoord.triggerCheckNamespaces("", 0, 0)
	}
	return nil
}

func (pdCoord *PDCoordinator) updateNamespaceMeta(currentNodes map[string]cluster.NodeInfo, namespace string, meta *cluster.NamespaceMetaInfo) error {
	cluster.CoordLog().Infof("update namespace: %v, with meta: %v", namespace, meta)

	if len(currentNodes) < meta.Replica {
		cluster.CoordLog().Infof("nodes %v is less than replica  %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	return pdCoord.register.UpdateNamespaceMetaInfo(namespace, meta, meta.MetaEpoch())
}

func (pdCoord *PDCoordinator) CreateNamespace(namespace string, meta cluster.NamespaceMetaInfo) error {
	if pdCoord.leaderNode.GetID() != pdCoord.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while create namespace")
		return ErrNotLeader
	}

	if !common.IsValidNamespaceName(namespace) {
		return errors.New("invalid namespace name")
	}

	if meta.PartitionNum >= common.MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	currentNodes := pdCoord.getCurrentNodes(meta.Tags)
	if len(currentNodes) < meta.Replica {
		cluster.CoordLog().Infof("nodes %v is less than replica %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if ok, _ := pdCoord.register.IsExistNamespace(namespace); !ok {
		meta.MagicCode = time.Now().UnixNano()
		var err error
		meta.MinGID, err = pdCoord.register.PrepareNamespaceMinGID()
		if err != nil {
			cluster.CoordLog().Infof("prepare namespace %v gid failed :%v", namespace, err)
			return err
		}
		err = pdCoord.register.CreateNamespace(namespace, &meta)
		if err != nil {
			cluster.CoordLog().Infof("create namespace key %v failed :%v", namespace, err)
			return err
		}
	} else {
		cluster.CoordLog().Warningf("namespace already exist :%v ", namespace)
		return ErrAlreadyExist
	}
	cluster.CoordLog().Infof("create namespace: %v, with meta: %v", namespace, meta)
	return pdCoord.checkAndUpdateNamespacePartitions(currentNodes, namespace, meta)
}

func (pdCoord *PDCoordinator) checkAndUpdateNamespacePartitions(currentNodes map[string]cluster.NodeInfo,
	namespace string, meta cluster.NamespaceMetaInfo) error {
	existPart := make(map[int]*cluster.PartitionMetaInfo)
	for i := 0; i < meta.PartitionNum; i++ {
		err := pdCoord.register.CreateNamespacePartition(namespace, i)
		if err != nil {
			cluster.CoordLog().Warningf("failed to create namespace %v-%v: %v", namespace, i, err)
			// handle already exist
			t, err := pdCoord.register.GetNamespacePartInfo(namespace, i)
			if err != nil {
				cluster.CoordLog().Warningf("exist namespace partition failed to get info: %v", err)
				if err != cluster.ErrKeyNotFound {
					return err
				}
			} else {
				cluster.CoordLog().Infof("create namespace partition already exist %v-%v", namespace, i)
				existPart[i] = t
			}
		}
	}
	partReplicaList, err := pdCoord.dpm.allocNamespaceRaftNodes(namespace, currentNodes, meta.Replica, meta.PartitionNum, existPart)
	if err != nil {
		cluster.CoordLog().Infof("failed to alloc nodes for namespace: %v", err)
		return err.ToErrorType()
	}
	if len(partReplicaList) != meta.PartitionNum {
		return ErrNodeUnavailable.ToErrorType()
	}
	for i := 0; i < meta.PartitionNum; i++ {
		if _, ok := existPart[i]; ok {
			continue
		}

		tmpReplicaInfo := partReplicaList[i]
		if len(tmpReplicaInfo.GetISR()) <= meta.Replica/2 {
			cluster.CoordLog().Infof("failed update info for namespace : %v-%v since not quorum", namespace, i, tmpReplicaInfo)
			continue
		}
		commonErr := pdCoord.register.UpdateNamespacePartReplicaInfo(namespace, i, &tmpReplicaInfo, tmpReplicaInfo.Epoch())
		if commonErr != nil {
			cluster.CoordLog().Infof("failed update info for namespace : %v-%v, %v", namespace, i, commonErr)
			continue
		}
	}
	pdCoord.triggerCheckNamespaces("", 0, time.Millisecond*500)
	return nil
}

func (pdCoord *PDCoordinator) AddHIndexSchema(namespace string, table string, hindex *common.HsetIndexSchema) error {
	hindex.State = common.InitIndex
	return pdCoord.addHIndexSchema(namespace, table, hindex)
}

func (pdCoord *PDCoordinator) DelHIndexSchema(namespace string, table string, hindexName string) error {
	return pdCoord.delHIndexSchema(namespace, table, hindexName)
}

func (pdCoord *PDCoordinator) RemoveLearnerFromNs(ns string, pidStr string, nid string) error {
	if pidStr == "**" {
		oldMeta, err := pdCoord.register.GetNamespaceMetaInfo(ns)
		if err != nil {
			cluster.CoordLog().Infof("get namespace key %v failed :%v", ns, err)
			return err
		}
		for i := 0; i < oldMeta.PartitionNum; i++ {
			err = pdCoord.removeNsLearnerFromNode(ns, i, nid)
			if err != nil {
				cluster.CoordLog().Infof("namespace %v-%v remove learner %v failed :%v", ns, i, nid, err)
				return err
			}
		}
		return nil
	}
	if pidStr == "" {
		return errors.New("missing partition")
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return err
	}
	return pdCoord.removeNsLearnerFromNode(ns, pid, nid)
}

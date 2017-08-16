package pdnode_coord

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
)

// some API for outside
func (self *PDCoordinator) IsClusterStable() bool {
	return atomic.LoadInt32(&self.isClusterUnstable) == 0 &&
		atomic.LoadInt32(&self.isUpgrading) == 0
}

func (self *PDCoordinator) IsMineLeader() bool {
	return self.leaderNode.GetID() == self.myNode.GetID()
}

func (self *PDCoordinator) GetAllPDNodes() ([]cluster.NodeInfo, error) {
	return self.register.GetAllPDNodes()
}

func (self *PDCoordinator) GetPDLeader() cluster.NodeInfo {
	return self.leaderNode
}

func (self *PDCoordinator) GetAllDataNodes() (map[string]cluster.NodeInfo, int64) {
	return self.getCurrentNodesWithRemoving()
}

func (self *PDCoordinator) GetAllNamespaces() (map[string]map[int]cluster.PartitionMetaInfo, int64, error) {
	ns, epoch, err := self.register.GetAllNamespaces()
	return ns, int64(epoch), err
}

func (self *PDCoordinator) SetClusterStableNodeNum(num int) error {
	if int32(num) > atomic.LoadInt32(&self.stableNodeNum) {
		return errors.New("cluster stable node number can not be increased by manunal, only decrease allowed")
	}
	atomic.StoreInt32(&self.stableNodeNum, int32(num))
	return nil
}

func (self *PDCoordinator) SetClusterUpgradeState(upgrading bool) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	if upgrading {
		if !atomic.CompareAndSwapInt32(&self.isUpgrading, 0, 1) {
			cluster.CoordLog().Infof("the cluster state is already upgrading")
			return nil
		}
		cluster.CoordLog().Infof("the cluster state has been changed to upgrading")
	} else {
		if !atomic.CompareAndSwapInt32(&self.isUpgrading, 1, 0) {
			return nil
		}
		cluster.CoordLog().Infof("the cluster state has been changed to normal")
		self.triggerCheckNamespaces("", 0, time.Second)
	}
	return nil
}

func (self *PDCoordinator) MarkNodeAsRemoving(nid string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	cluster.CoordLog().Infof("try mark node %v as removed", nid)
	self.nodesMutex.Lock()
	newRemovingNodes := make(map[string]string)
	if _, ok := self.removingNodes[nid]; ok {
		cluster.CoordLog().Infof("already mark as removing")
	} else {
		newRemovingNodes[nid] = "marked"
		for id, removeState := range self.removingNodes {
			newRemovingNodes[id] = removeState
		}
		self.removingNodes = newRemovingNodes
	}
	self.nodesMutex.Unlock()
	return nil
}

func (self *PDCoordinator) DeleteNamespace(ns string, partition string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while delete namespace")
		return ErrNotLeader
	}

	begin := time.Now()
	for !atomic.CompareAndSwapInt32(&self.doChecking, 0, 1) {
		cluster.CoordLog().Infof("delete %v waiting check namespace finish", ns)
		time.Sleep(time.Millisecond * 200)
		if time.Since(begin) > time.Second*5 {
			return ErrClusterUnstable
		}
	}
	defer atomic.StoreInt32(&self.doChecking, 0)
	cluster.CoordLog().Infof("delete namespace: %v, with partition: %v", ns, partition)
	if ok, err := self.register.IsExistNamespace(ns); !ok {
		cluster.CoordLog().Infof("no namespace : %v", err)
		return cluster.ErrKeyNotFound
	}

	if partition == "**" {
		// delete all
		meta, err := self.register.GetNamespaceMetaInfo(ns)
		if err != nil {
			cluster.CoordLog().Infof("failed to get meta for namespace: %v", err)
		}
		for pid := 0; pid < meta.PartitionNum; pid++ {
			err := self.deleteNamespacePartition(ns, pid)
			if err != nil {
				cluster.CoordLog().Infof("failed to delete namespace partition %v for namespace: %v, err:%v", pid, ns, err)
			}
		}
		err = self.register.DeleteWholeNamespace(ns)
		if err != nil {
			cluster.CoordLog().Infof("failed to delete whole namespace: %v : %v", ns, err)
		}
	}
	return nil
}

func (self *PDCoordinator) deleteNamespacePartition(namespace string, pid int) error {
	commonErr := self.register.DeleteNamespacePart(namespace, pid)
	if commonErr != nil {
		cluster.CoordLog().Infof("failed to delete the namespace info : %v", commonErr)
		return commonErr
	}
	return nil
}

func (self *PDCoordinator) ChangeNamespaceMetaParam(namespace string, newReplicator int,
	optimizeFsync string, snapCount int) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
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
	if ok, _ := self.register.IsExistNamespace(namespace); !ok {
		cluster.CoordLog().Infof("namespace not exist %v :%v", namespace)
		return cluster.ErrNamespaceNotCreated.ToErrorType()
	} else {
		oldMeta, err := self.register.GetNamespaceMetaInfo(namespace)
		if err != nil {
			cluster.CoordLog().Infof("get namespace key %v failed :%v", namespace, err)
			return err
		}
		currentNodes := self.getCurrentNodes(oldMeta.Tags)
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
		err = self.updateNamespaceMeta(currentNodes, namespace, &meta)
		if err != nil {
			return err
		}
		self.triggerCheckNamespaces("", 0, 0)
	}
	return nil
}

func (self *PDCoordinator) updateNamespaceMeta(currentNodes map[string]cluster.NodeInfo, namespace string, meta *cluster.NamespaceMetaInfo) error {
	cluster.CoordLog().Infof("update namespace: %v, with meta: %v", namespace, meta)

	if len(currentNodes) < meta.Replica {
		cluster.CoordLog().Infof("nodes %v is less than replica  %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	return self.register.UpdateNamespaceMetaInfo(namespace, meta, meta.MetaEpoch())
}

func (self *PDCoordinator) CreateNamespace(namespace string, meta cluster.NamespaceMetaInfo) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		cluster.CoordLog().Infof("not leader while create namespace")
		return ErrNotLeader
	}

	if !common.IsValidNamespaceName(namespace) {
		return errors.New("invalid namespace name")
	}

	if meta.PartitionNum >= common.MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	currentNodes := self.getCurrentNodes(meta.Tags)
	if len(currentNodes) < meta.Replica {
		cluster.CoordLog().Infof("nodes %v is less than replica %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if ok, _ := self.register.IsExistNamespace(namespace); !ok {
		meta.MagicCode = time.Now().UnixNano()
		var err error
		meta.MinGID, err = self.register.PrepareNamespaceMinGID()
		if err != nil {
			cluster.CoordLog().Infof("prepare namespace %v gid failed :%v", namespace, err)
			return err
		}
		err = self.register.CreateNamespace(namespace, &meta)
		if err != nil {
			cluster.CoordLog().Infof("create namespace key %v failed :%v", namespace, err)
			return err
		}
	} else {
		cluster.CoordLog().Warningf("namespace already exist :%v ", namespace)
		return ErrAlreadyExist
	}
	cluster.CoordLog().Infof("create namespace: %v, with meta: %v", namespace, meta)
	return self.checkAndUpdateNamespacePartitions(currentNodes, namespace, meta)
}

func (self *PDCoordinator) checkAndUpdateNamespacePartitions(currentNodes map[string]cluster.NodeInfo,
	namespace string, meta cluster.NamespaceMetaInfo) error {
	existPart := make(map[int]*cluster.PartitionMetaInfo)
	for i := 0; i < meta.PartitionNum; i++ {
		err := self.register.CreateNamespacePartition(namespace, i)
		if err != nil {
			cluster.CoordLog().Warningf("failed to create namespace %v-%v: %v", namespace, i, err)
			// handle already exist
			t, err := self.register.GetNamespacePartInfo(namespace, i)
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
	partReplicaList, err := self.dpm.allocNamespaceRaftNodes(namespace, currentNodes, meta.Replica, meta.PartitionNum, existPart)
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
		commonErr := self.register.UpdateNamespacePartReplicaInfo(namespace, i, &tmpReplicaInfo, tmpReplicaInfo.Epoch())
		if commonErr != nil {
			cluster.CoordLog().Infof("failed update info for namespace : %v-%v, %v", namespace, i, commonErr)
			continue
		}
	}
	self.triggerCheckNamespaces("", 0, time.Millisecond*500)
	return nil
}

func (self *PDCoordinator) AddHIndexSchema(namespace string, table string, hindex *common.HsetIndexSchema) error {
	hindex.State = common.InitIndex
	return self.addHIndexSchema(namespace, table, hindex)
}

func (self *PDCoordinator) DelHIndexSchema(namespace string, table string, hindexName string) error {
	return self.delHIndexSchema(namespace, table, hindexName)
}

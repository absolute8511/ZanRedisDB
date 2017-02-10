package cluster

import (
	"sync"
	"sync/atomic"
)

type coordData struct {
	namespaceInfo PartitionMetaInfo
	forceLeave    int32
}

func (self *coordData) GetCopy() *coordData {
	newCoordData := &coordData{}
	if self == nil {
		return newCoordData
	}
	*newCoordData = *self
	return newCoordData
}

type NamespaceCoordinator struct {
	dataMutex sync.Mutex
	*coordData
	// hold for write to avoid disable or exiting or catchup
	// lock order: first lock writehold then lock data to avoid deadlock
	writeHold      sync.Mutex
	catchupRunning int32
	exiting        int32
}

func NewNamespaceCoordinator(name string, partition int) (*NamespaceCoordinator, error) {
	tc := &NamespaceCoordinator{}
	tc.coordData = &coordData{}
	tc.namespaceInfo.Name = name
	tc.namespaceInfo.Partition = partition
	return tc, nil
}

func (self *NamespaceCoordinator) Delete(removeData bool) {
	self.Exiting()
	self.SetForceLeave(true)
}

func (self *NamespaceCoordinator) GetData() *coordData {
	self.dataMutex.Lock()
	d := self.coordData
	self.dataMutex.Unlock()
	return d
}

func (self *NamespaceCoordinator) IsWriteDisabled() bool {
	return false
}

func (self *NamespaceCoordinator) IsExiting() bool {
	return atomic.LoadInt32(&self.exiting) == 1
}

func (self *NamespaceCoordinator) Exiting() {
	atomic.StoreInt32(&self.exiting, 1)
}

func (self *coordData) IsMineISR(id string) bool {
	return FindSlice(self.namespaceInfo.RaftNodes, id) != -1
}

func (self *coordData) IsISRReadyForWrite() bool {
	return len(self.namespaceInfo.RaftNodes) > self.namespaceInfo.Replica/2
}

func (self *coordData) SetForceLeave(leave bool) {
	if leave {
		atomic.StoreInt32(&self.forceLeave, 1)
	} else {
		atomic.StoreInt32(&self.forceLeave, 0)
	}
}

func (self *coordData) IsForceLeave() bool {
	return atomic.LoadInt32(&self.forceLeave) == 1
}

package rockredis

import (
	"github.com/absolute8511/gorocksdb"
	"sync"
)

type JsonIndex struct {
}

type TableIndexContainer struct {
	sync.RWMutex
	// field -> index name, to convert "secondaryindex.select * from table where field = xxx" to scan(/hindex/table/indexname/xxx)
	hsetIndexes map[string]*HsetIndex
	jsonIndexes map[string]*JsonIndex
}

func NewIndexContainer() *TableIndexContainer {
	return &TableIndexContainer{
		hsetIndexes: make(map[string]*HsetIndex),
		jsonIndexes: make(map[string]*JsonIndex),
	}
}

type IndexMgr struct {
	sync.RWMutex
	tableIndexes map[string]*TableIndexContainer
}

func NewIndexMgr() *IndexMgr {
	return &IndexMgr{
		tableIndexes: make(map[string]*TableIndexContainer),
	}
}

func (self *IndexMgr) LoadIndexes(db *gorocksdb.DB) error {
	dbLog.Infof("begin loading indexes...")
	defer dbLog.Infof("finish load indexes.")
	return nil
}

func (self *IndexMgr) Close() {
	self.Lock()
	self.tableIndexes = make(map[string]*TableIndexContainer)
	self.Unlock()
}

func (self *IndexMgr) AddHsetIndex(hindex *HsetIndex) error {
	self.Lock()
	defer self.Unlock()
	indexes, ok := self.tableIndexes[string(hindex.Table)]
	if !ok {
		indexes = NewIndexContainer()
		self.tableIndexes[string(hindex.Table)] = indexes
	}
	indexes.Lock()
	defer indexes.Unlock()
	_, ok = indexes.hsetIndexes[string(hindex.IndexField)]
	if ok {
		return ErrIndexExist
	}
	hindex.State = InitIndex
	indexes.hsetIndexes[string(hindex.IndexField)] = hindex
	return nil
}

func (self *IndexMgr) UpdateHsetIndexState(table string, field string, state TableIndexState) error {
	self.RLock()
	defer self.RUnlock()
	indexes, ok := self.tableIndexes[table]
	if !ok {
		return ErrIndexNotExist
	}

	indexes.Lock()
	defer indexes.Unlock()
	index, ok := indexes.hsetIndexes[field]
	if !ok {
		return ErrIndexNotExist
	}
	index.State = state
	return nil
}

func (self *IndexMgr) DeleteHsetIndex(table string, field string) error {
	self.Lock()
	defer self.Unlock()
	indexes, ok := self.tableIndexes[table]
	if !ok {
		return ErrIndexNotExist
	}

	indexes.Lock()
	defer indexes.Unlock()
	_, ok = indexes.hsetIndexes[field]
	if !ok {
		return ErrIndexNotExist
	}
	delete(indexes.hsetIndexes, field)
	return nil
}

func (self *IndexMgr) GetHsetIndex(table string, field string) (*HsetIndex, error) {
	self.RLock()
	defer self.RUnlock()
	indexes, ok := self.tableIndexes[table]
	if !ok {
		return nil, ErrIndexNotExist
	}

	indexes.Lock()
	defer indexes.Unlock()
	index, ok := indexes.hsetIndexes[field]
	if !ok {
		return nil, ErrIndexNotExist
	}

	return index, nil
}

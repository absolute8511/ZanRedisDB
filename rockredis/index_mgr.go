package rockredis

import (
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

func (self *TableIndexContainer) marshalHsetIndexes() ([]byte, error) {
	var indexList HsetIndexList
	for _, v := range self.hsetIndexes {
		indexList.HsetIndexes = append(indexList.HsetIndexes, v.HsetIndexInfo)
	}
	return indexList.Marshal()
}

func (self *TableIndexContainer) unmarshalHsetIndexes(table []byte, data []byte) error {
	var indexList HsetIndexList
	err := indexList.Unmarshal(data)
	if err != nil {
		return err
	}
	self.hsetIndexes = make(map[string]*HsetIndex)
	for _, v := range indexList.HsetIndexes {
		var hi HsetIndex
		hi.HsetIndexInfo = v
		hi.Table = table
		self.hsetIndexes[string(v.IndexField)] = &hi
	}
	dbLog.Infof("load hash index: %v", indexList.String())
	return nil
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

func (self *IndexMgr) LoadIndexes(db *RockDB) error {
	dbLog.Infof("begin loading indexes...")
	defer dbLog.Infof("finish load indexes.")
	tables := db.GetHsetIndexTables()
	for t := range tables {
		d, err := db.GetTableHsetIndexValue(t)
		if err != nil {
			dbLog.Infof("get table %v hset index failed: %v", string(t), err)
			continue
		}
		if d == nil {
			dbLog.Infof("get table %v hset index empty", string(t))
			continue
		}
		indexes := NewIndexContainer()
		err = indexes.unmarshalHsetIndexes(t, d)
		if err != nil {
			dbLog.Infof("unmarshal table %v hset indexes failed: %v", string(t), err)
			return err
		}
		dbLog.Infof("table %v load %v hash indexes", string(t), len(indexes.hsetIndexes))
		self.tableIndexes[string(t)] = indexes
	}
	return nil
}

func (self *IndexMgr) Close() {
	self.Lock()
	self.tableIndexes = make(map[string]*TableIndexContainer)
	self.Unlock()
}

func (self *IndexMgr) AddHsetIndex(db *RockDB, hindex *HsetIndex) error {
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
	d, err := indexes.marshalHsetIndexes()
	if err != nil {
		delete(indexes.hsetIndexes, string(hindex.IndexField))
		return err
	}
	err = db.SetTableHsetIndexValue(hindex.Table, d)
	if err != nil {
		delete(indexes.hsetIndexes, string(hindex.IndexField))
		return err
	}
	return err
}

func (self *IndexMgr) UpdateHsetIndexState(db *RockDB, table string, field string, state IndexState) error {
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
	oldState := index.State
	index.State = state
	d, err := indexes.marshalHsetIndexes()
	if err != nil {
		index.State = oldState
		return err
	}
	err = db.SetTableHsetIndexValue([]byte(table), d)
	if err != nil {
		index.State = oldState
		return err
	}

	return nil
}

func (self *IndexMgr) DeleteHsetIndex(db *RockDB, table string, field string) error {
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
	d, err := indexes.marshalHsetIndexes()
	if err != nil {
		return err
	}
	err = db.SetTableHsetIndexValue([]byte(table), d)
	if err != nil {
		return err
	}
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

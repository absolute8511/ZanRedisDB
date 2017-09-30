package rockredis

import (
	"bytes"
	"errors"
	"sync"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
)

var (
	ErrIndexStateInvalidChange = errors.New("index state change state is not invalid")
	ErrIndexDeleteNotInDeleted = errors.New("delete index in wrong state")
	ErrIndexClosed             = errors.New("index is closed")
)

var (
	buildIndexBlock = 1000
)

type JSONIndex struct {
	Table []byte
	HsetIndexInfo
}

type TableIndexContainer struct {
	sync.RWMutex
	// field -> index name, to convert "secondaryindex.select * from table where field = xxx" to scan(/hindex/table/indexname/xxx)
	hsetIndexes map[string]*HsetIndex
	jsonIndexes map[string]*JSONIndex
}

func NewIndexContainer() *TableIndexContainer {
	return &TableIndexContainer{
		hsetIndexes: make(map[string]*HsetIndex),
		jsonIndexes: make(map[string]*JSONIndex),
	}
}

func (tic *TableIndexContainer) GetHIndexNoLock(field string) *HsetIndex {
	index, ok := tic.hsetIndexes[field]
	if !ok {
		return nil
	}
	if index.State == InitIndex {
		return nil
	}
	return index
}

func (tic *TableIndexContainer) marshalHsetIndexes() ([]byte, error) {
	var indexList HsetIndexList
	for _, v := range tic.hsetIndexes {
		indexList.HsetIndexes = append(indexList.HsetIndexes, v.HsetIndexInfo)
	}
	return indexList.Marshal()
}

func (tic *TableIndexContainer) unmarshalHsetIndexes(table []byte, data []byte) error {
	var indexList HsetIndexList
	err := indexList.Unmarshal(data)
	if err != nil {
		return err
	}
	tic.hsetIndexes = make(map[string]*HsetIndex)
	for _, v := range indexList.HsetIndexes {
		var hi HsetIndex
		hi.HsetIndexInfo = v
		hi.Table = table
		tic.hsetIndexes[string(v.IndexField)] = &hi
	}
	dbLog.Infof("load hash index: %v", indexList.String())
	return nil
}

func (tic *TableIndexContainer) GetJSONIndexNoLock(path string) *JSONIndex {
	// TODO: path need to be converted to canonical form
	index, ok := tic.jsonIndexes[path]
	if !ok {
		return nil
	}
	if index.State == InitIndex {
		return nil
	}
	return index
}

type IndexMgr struct {
	sync.RWMutex
	tableIndexes   map[string]*TableIndexContainer
	closeChan      chan struct{}
	indexBuildChan chan int
	wg             sync.WaitGroup
}

func NewIndexMgr() *IndexMgr {
	return &IndexMgr{
		tableIndexes:   make(map[string]*TableIndexContainer),
		indexBuildChan: make(chan int, 10),
	}
}

func (im *IndexMgr) GetAllIndexSchemaInfo(db *RockDB) (map[string]*common.IndexSchema, error) {
	im.RLock()
	defer im.RUnlock()
	isClosed := im.closeChan == nil
	if isClosed {
		return nil, ErrIndexClosed
	}
	schemas := make(map[string]*common.IndexSchema)
	for name, t := range im.tableIndexes {
		var schema common.IndexSchema
		t.RLock()
		for _, v := range t.hsetIndexes {
			schema.HsetIndexes = append(schema.HsetIndexes, &common.HsetIndexSchema{
				Name:       string(v.Name),
				IndexField: string(v.IndexField),
				PrefixLen:  v.PrefixLen,
				Unique:     v.Unique,
				ValueType:  common.IndexPropertyDType(v.ValueType),
				State:      common.IndexState(v.State),
			})
		}
		//for _, v := range t.jsonIndexes {
		//	schema.JSONIndexes = append(schema.JSONIndexes, common.JSONIndexSchema{})
		//}
		t.RUnlock()
		schemas[name] = &schema
	}
	return schemas, nil
}

func (im *IndexMgr) GetIndexSchemaInfo(db *RockDB, table string) (*common.IndexSchema, error) {
	im.RLock()
	defer im.RUnlock()
	isClosed := im.closeChan == nil
	if isClosed {
		return nil, ErrIndexClosed
	}

	var schema common.IndexSchema
	t, ok := im.tableIndexes[table]
	if !ok {
		return nil, ErrIndexTableNotExist
	}
	t.RLock()
	for _, v := range t.hsetIndexes {
		schema.HsetIndexes = append(schema.HsetIndexes, &common.HsetIndexSchema{
			Name:       string(v.Name),
			IndexField: string(v.IndexField),
			PrefixLen:  v.PrefixLen,
			Unique:     v.Unique,
			ValueType:  common.IndexPropertyDType(v.ValueType),
			State:      common.IndexState(v.State),
		})
	}
	//for _, v := range t.jsonIndexes {
	//	schema.JSONIndexes = append(schema.JSONIndexes, common.JSONIndexSchema{})
	//}
	t.RUnlock()
	return &schema, nil
}

func (im *IndexMgr) LoadIndexes(db *RockDB) error {
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
		im.Lock()
		im.tableIndexes[string(t)] = indexes
		im.Unlock()
	}

	im.Lock()
	if im.closeChan != nil {
		select {
		case <-im.closeChan:
		default:
			close(im.closeChan)
		}
	}
	im.closeChan = make(chan struct{})
	im.wg.Add(1)
	go func(stopC chan struct{}) {
		defer im.wg.Done()
		im.buildIndexes(db, stopC)
	}(im.closeChan)
	im.Unlock()
	select {
	case im.indexBuildChan <- 1:
	default:
	}
	return nil
}

func (im *IndexMgr) Close() {
	dbLog.Infof("closing index manager")
	im.Lock()
	im.tableIndexes = make(map[string]*TableIndexContainer)
	if im.closeChan != nil {
		select {
		case <-im.closeChan:
		default:
			close(im.closeChan)
		}
	}
	im.Unlock()
	im.wg.Wait()
	dbLog.Infof("index manager closed")
}

func (im *IndexMgr) AddHsetIndex(db *RockDB, hindex *HsetIndex) error {
	im.Lock()
	indexes, ok := im.tableIndexes[string(hindex.Table)]
	if !ok {
		indexes = NewIndexContainer()
		im.tableIndexes[string(hindex.Table)] = indexes
	}
	im.Unlock()
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
		indexes.hsetIndexes[string(hindex.IndexField)] = nil
		delete(indexes.hsetIndexes, string(hindex.IndexField))
		return err
	}
	err = db.SetTableHsetIndexValue(hindex.Table, d)
	if err != nil {
		indexes.hsetIndexes[string(hindex.IndexField)] = nil
		delete(indexes.hsetIndexes, string(hindex.IndexField))
		return err
	}
	dbLog.Infof("table %v add hash index %v", string(hindex.Table), hindex.String())
	return err
}

func (im *IndexMgr) UpdateHsetIndexState(db *RockDB, table string, field string, state IndexState) error {
	im.RLock()
	isClosed := im.closeChan == nil
	indexes, ok := im.tableIndexes[table]
	im.RUnlock()
	if !ok {
		return ErrIndexTableNotExist
	}
	if isClosed {
		return ErrIndexClosed
	}

	indexes.Lock()
	defer indexes.Unlock()
	index, ok := indexes.hsetIndexes[field]
	if !ok {
		return ErrIndexNotExist
	}
	if index.State == state {
		return nil
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
	dbLog.Infof("table %v hash index %v state updated from %v to %v", table, field, oldState, state)
	if index.State == DeletedIndex {
		im.wg.Add(1)
		go func() {
			defer im.wg.Done()
			err := index.cleanAll(db, im.closeChan)
			if err != nil {
				dbLog.Infof("failed to clean index: %v", err)
			} else {
				im.deleteHsetIndex(db, string(index.Table), string(index.IndexField))
			}
		}()
	} else if index.State == BuildingIndex {
		select {
		case im.indexBuildChan <- 1:
		default:
		}
	}

	return nil
}

// ensure mark index as deleted, and clean in background before delete the index
func (im *IndexMgr) deleteHsetIndex(db *RockDB, table string, field string) error {
	// this delete may not run in raft loop
	// so we should use new db write batch
	im.Lock()
	indexes, ok := im.tableIndexes[table]
	im.Unlock()
	if !ok {
		return ErrIndexTableNotExist
	}

	indexes.Lock()
	defer indexes.Unlock()
	hindex, ok := indexes.hsetIndexes[field]
	if !ok {
		return ErrIndexNotExist
	}
	if hindex.State != DeletedIndex {
		return ErrIndexDeleteNotInDeleted
	}
	indexes.hsetIndexes[field] = nil
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

func (im *IndexMgr) GetTableIndexes(table string) *TableIndexContainer {
	im.RLock()
	indexes, ok := im.tableIndexes[table]
	im.RUnlock()
	if !ok {
		return nil
	}
	return indexes
}

func (im *IndexMgr) GetHsetIndex(table string, field string) (*HsetIndex, error) {
	im.RLock()
	indexes, ok := im.tableIndexes[table]
	im.RUnlock()
	if !ok {
		dbLog.Infof("current index %v", im.tableIndexes)
		return nil, ErrIndexTableNotExist
	}

	indexes.Lock()
	defer indexes.Unlock()
	index, ok := indexes.hsetIndexes[field]
	if !ok {
		return nil, ErrIndexNotExist
	}

	return index, nil
}

func (im *IndexMgr) buildIndexes(db *RockDB, stopChan chan struct{}) {
	for {
		select {
		case <-im.indexBuildChan:
			im.dobuildIndexes(db, stopChan)
		case <-stopChan:
			return
		}
	}
}

func (im *IndexMgr) dobuildIndexes(db *RockDB, stopChan chan struct{}) {
	var buildWg sync.WaitGroup
	im.Lock()
	for table, v := range im.tableIndexes {
		tmpHsetIndexes := make([]*HsetIndex, 0)
		v.RLock()
		for _, hindex := range v.hsetIndexes {
			if hindex.State == BuildingIndex {
				tmpHsetIndexes = append(tmpHsetIndexes, hindex)
			}
		}
		v.RUnlock()
		if len(tmpHsetIndexes) == 0 {
			continue
		}
		dbLog.Infof("begin rebuild index for table %v", table)
		fields := make([][]byte, 0)
		for _, hindex := range tmpHsetIndexes {
			fields = append(fields, hindex.IndexField)
			dbLog.Infof("begin rebuild index for field: %s", string(hindex.IndexField))
		}

		buildWg.Add(1)
		go func(buildTable string, t *TableIndexContainer) {
			defer buildWg.Done()
			cursor := []byte(buildTable)
			cursor = append(cursor, common.NamespaceTableSeperator)
			origPrefix := cursor
			indexPKCnt := 0
			pkList := make([][]byte, 0, buildIndexBlock)
			for {
				done, err := func() (bool, error) {
					t.Lock()
					defer t.Unlock()
					select {
					case <-stopChan:
						dbLog.Infof("rebuild index for table %v stopped", buildTable)
						return true, ErrIndexClosed
					default:
					}

					if cap(pkList) < buildIndexBlock {
						pkList = make([][]byte, 0, buildIndexBlock)
					}
					pkList = pkList[:0]
					var err error
					pkList, err = db.ScanWithBuffer(common.HASH, cursor, buildIndexBlock, "", pkList)
					if err != nil {
						dbLog.Infof("rebuild index for table %v error %v", buildTable, err)
						return true, err
					}
					wb := gorocksdb.NewWriteBatch()
					for _, pk := range pkList {
						if !bytes.HasPrefix(pk, origPrefix) {
							dbLog.Infof("rebuild index for table %v end at: %v, next is: %v",
								buildTable, string(cursor), string(pk))
							cursor = nil
							break
						}
						values, err := db.HMget(pk, fields...)
						if err != nil {
							dbLog.Infof("rebuild index for table %v error %v ", buildTable, err)
							return true, err
						}
						for i := range fields {
							err = tmpHsetIndexes[i].UpdateRec(nil, values[i], pk, wb)
							if err != nil {
								dbLog.Infof("rebuild index for table %v error %v ", buildTable, err)
								return true, err
							}
						}
						cursor = pk
						indexPKCnt++
					}
					if len(pkList) < buildIndexBlock {
						cursor = nil
					}
					db.eng.Write(db.defaultWriteOpts, wb)
					wb.Destroy()
					if len(cursor) == 0 {
						return true, nil
					} else {
						dbLog.Infof("rebuilding index for table %v current cursor: %s, cnt: %v",
							buildTable, string(cursor), indexPKCnt)
					}
					return false, nil
				}()
				if done {
					dbLog.Infof("finish rebuild index for table %v, total: %v", string(buildTable), indexPKCnt)
					t.Lock()
					for _, f := range fields {
						hindex, ok := t.hsetIndexes[string(f)]
						if ok {
							if err != nil {
								hindex.State = InitIndex
							} else {
								hindex.State = BuildDoneIndex
							}
						}
					}
					t.Unlock()
					break
				}
			}
		}(table, v)
	}
	im.Unlock()

	buildWg.Wait()
}

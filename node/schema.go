package node

import (
	"encoding/json"
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
)

func (self *KVNode) GetIndexSchema(table string) (map[string]*common.IndexSchema, error) {
	if len(table) == 0 {
		return self.store.GetAllIndexSchema()
	}
	schema, err := self.store.GetIndexSchema(table)
	if err != nil {
		return nil, err
	}
	return map[string]*common.IndexSchema{
		table: schema,
	}, nil
}

func (self *KVNode) handleSchemaUpdate(sc SchemaChange) error {
	switch sc.Type {
	case SchemaChangeAddHsetIndex, SchemaChangeUpdateHsetIndex, SchemaChangeDeleteHsetIndex:
		var hindex common.HsetIndexSchema
		err := json.Unmarshal(sc.SchemaData, &hindex)
		if err != nil {
			return err
		}
		if sc.Type == SchemaChangeAddHsetIndex {
			err = self.store.AddHsetIndex(sc.Table, &hindex)
		} else {
			err = self.store.UpdateHsetIndexState(sc.Table, &hindex)
		}
		return err
	default:
		return errors.New("unknown schema change type")
	}
}

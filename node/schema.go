package node

import (
	"encoding/json"
	"errors"

	"github.com/youzan/ZanRedisDB/common"
)

func (nd *KVNode) GetIndexSchema(table string) (map[string]*common.IndexSchema, error) {
	if len(table) == 0 {
		return nd.store.GetAllIndexSchema()
	}
	schema, err := nd.store.GetIndexSchema(table)
	if err != nil {
		return nil, err
	}
	return map[string]*common.IndexSchema{
		table: schema,
	}, nil
}

func (kvsm *kvStoreSM) handleSchemaUpdate(sc SchemaChange) error {
	switch sc.Type {
	case SchemaChangeAddHsetIndex, SchemaChangeUpdateHsetIndex, SchemaChangeDeleteHsetIndex:
		var hindex common.HsetIndexSchema
		err := json.Unmarshal(sc.SchemaData, &hindex)
		if err != nil {
			return err
		}
		if sc.Type == SchemaChangeAddHsetIndex {
			err = kvsm.store.AddHsetIndex(sc.Table, &hindex)
		} else {
			err = kvsm.store.UpdateHsetIndexState(sc.Table, &hindex)
		}
		return err
	default:
		return errors.New("unknown schema change type")
	}
}

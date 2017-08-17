package pdnode_coord

import (
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
)

var (
	ErrInvalidSchema = errors.New("invalid schema info")
)

func getIndexSchemasFromDataNode(remoteNode string, ns string) (map[string]*common.IndexSchema, error) {
	nip, _, _, httpPort := cluster.ExtractNodeInfoFromID(remoteNode)
	rsp := make(map[string]*common.IndexSchema)
	_, err := common.APIRequest("GET",
		"http://"+net.JoinHostPort(nip, httpPort)+common.APIGetIndexes+"/"+ns,
		nil, time.Second*3, &rsp)
	if err != nil {
		cluster.CoordLog().Infof("failed (%v) to get indexes for namespace %v : %v",
			nip, ns, err)
		return nil, err
	}
	return rsp, nil
}

func isAllPartsIndexSchemaReady(allPartsSchema map[int]map[string]*common.IndexSchema, table string,
	name string, expectedState common.IndexState) bool {
	allDone := true
	for _, partSchema := range allPartsSchema {
		s, ok := partSchema[table]
		if !ok {
			allDone = false
			break
		}
		isSame := false
		for _, partIndex := range s.HsetIndexes {
			if partIndex.Name == name {
				if partIndex.State == expectedState {
					isSame = true
				}
				break
			}
		}
		if !isSame {
			allDone = false
			break
		}
	}
	return allDone
}

func (pdCoord *PDCoordinator) doSchemaCheck() {
	allNamespaces, _, err := pdCoord.register.GetAllNamespaces()
	if err != nil {
		return
	}
	for ns, parts := range allNamespaces {
		schemas, err := pdCoord.register.GetNamespaceSchemas(ns)
		if err != nil {
			if err != cluster.ErrKeyNotFound {
				cluster.CoordLog().Infof("get schema info failed: %v", err)
			}
			continue
		}
		if len(parts) == 0 || len(parts) != parts[0].PartitionNum {
			continue
		}
		allPartsSchema := make(map[int]map[string]*common.IndexSchema)
		isReady := true
		for pid, part := range parts {
			if len(part.RaftNodes) == 0 {
				isReady = false
				break
			}
			schemas, err := getIndexSchemasFromDataNode(part.RaftNodes[0],
				common.GetNsDesp(ns, pid))
			if err != nil {
				isReady = false
				break
			}
			cluster.CoordLog().Debugf("namespace %v got schema : %v", common.GetNsDesp(ns, pid),
				len(schemas))
			allPartsSchema[pid] = schemas
		}
		if !isReady || len(allPartsSchema) != parts[0].PartitionNum {
			continue
		}
		for table, schemaInfo := range schemas {
			var indexes common.IndexSchema
			err := json.Unmarshal(schemaInfo.Schema, &indexes)
			if err != nil {
				cluster.CoordLog().Infof("unmarshal schema data failed: %v", err)
				continue
			}
			schemaChanged := false
			newSchemaInfo := schemaInfo
			for _, hindex := range indexes.HsetIndexes {
				switch hindex.State {
				case common.InitIndex:
					// wait all partitions to begin init index, and then change the state to building
					if isAllPartsIndexSchemaReady(allPartsSchema, table, hindex.Name, common.InitIndex) {
						schemaChanged = true
						hindex.State = common.BuildingIndex
					}
					go pdCoord.triggerCheckNamespaces(ns, -1, time.Second)
				case common.BuildingIndex:
					// wait all partitions to finish building index, and then change the state to ready
					if isAllPartsIndexSchemaReady(allPartsSchema, table, hindex.Name, common.BuildDoneIndex) {
						schemaChanged = true
						hindex.State = common.ReadyIndex
						cluster.CoordLog().Infof("namespace %v table %v schema info ready: %v", ns, table, hindex)
					} else {
						go pdCoord.triggerCheckNamespaces(ns, -1, time.Second*3)
					}
				default:
				}
			}
			for _, jsonIndex := range indexes.JsonIndexes {
				_ = jsonIndex
			}

			if schemaChanged {
				newSchemaInfo.Schema, _ = json.Marshal(indexes)
				cluster.CoordLog().Infof("namespace %v table %v schema info changed: %v", ns, table, string(newSchemaInfo.Schema))
				err := pdCoord.register.UpdateNamespaceSchema(ns, table, &newSchemaInfo)
				if err != nil {
					cluster.CoordLog().Infof("update %v-%v schema to register failed: %v", ns, table, err)
				}
			}
		}
	}
}

func (pdCoord *PDCoordinator) addHIndexSchema(ns string, table string, hindex *common.HsetIndexSchema) error {
	if !hindex.IsValidNewSchema() {
		return ErrInvalidSchema
	}
	var indexes common.IndexSchema
	var newSchema cluster.SchemaInfo

	schema, err := pdCoord.register.GetNamespaceTableSchema(ns, table)
	if err != nil {
		if err != cluster.ErrKeyNotFound {
			return err
		}
		newSchema.Epoch = 0
	} else {
		newSchema.Epoch = schema.Epoch
		err := json.Unmarshal(schema.Schema, &indexes)
		if err != nil {
			cluster.CoordLog().Infof("unmarshal schema data failed: %v", err)
			return err
		}
	}

	for _, hi := range indexes.HsetIndexes {
		if hi.Name == hindex.Name {
			return errors.New("index already exist")
		}
	}
	indexes.HsetIndexes = append(indexes.HsetIndexes, hindex)
	newSchema.Schema, _ = json.Marshal(indexes)
	return pdCoord.register.UpdateNamespaceSchema(ns, table, &newSchema)
}

func (pdCoord *PDCoordinator) delHIndexSchema(ns string, table string, hindexName string) error {
	var indexes common.IndexSchema
	var newSchema cluster.SchemaInfo

	schema, err := pdCoord.register.GetNamespaceTableSchema(ns, table)
	if err != nil {
		return err
	}
	newSchema.Epoch = schema.Epoch
	err = json.Unmarshal(schema.Schema, &indexes)
	if err != nil {
		cluster.CoordLog().Infof("unmarshal schema data failed: %v", err)
		return err
	}
	for _, hi := range indexes.HsetIndexes {
		if hi.Name == hindexName {
			if hi.State != common.ReadyIndex {
				cluster.CoordLog().Infof("namespace %v table %v index schema not ready: %v", ns, table, hi)
				return errors.New("Unready index can not be deleted")
			}
			cluster.CoordLog().Infof("namespace %v table %v index schema deleted: %v", ns, table, hi)
			hi.State = common.DeletedIndex
		}
	}
	newSchema.Schema, _ = json.Marshal(indexes)
	return pdCoord.register.UpdateNamespaceSchema(ns, table, &newSchema)
}

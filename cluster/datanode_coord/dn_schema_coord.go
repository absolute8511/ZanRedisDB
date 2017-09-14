package datanode_coord

import (
	"encoding/json"

	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	node "github.com/absolute8511/ZanRedisDB/node"
)

// this will only be handled on leader of raft group
func (dc *DataCoordinator) doSyncSchemaInfo(localNamespace *node.NamespaceNode,
	indexSchemas map[string]*common.IndexSchema) {

	cluster.CoordLog().Debugf("namespace %v checking schema sync: %v",
		localNamespace.FullName(), len(indexSchemas))
	for table, tindexes := range indexSchemas {
		localIndexSchema, err := localNamespace.Node.GetIndexSchema(table)
		schemaMap := make(map[string]*common.HsetIndexSchema)
		if err == nil {
			localTableIndexSchema, ok := localIndexSchema[table]
			if ok {
				for _, v := range localTableIndexSchema.HsetIndexes {
					schemaMap[v.Name] = v
				}
			}
		}
		for _, hindex := range tindexes.HsetIndexes {
			sc := &node.SchemaChange{
				Type:       node.SchemaChangeAddHsetIndex,
				Table:      table,
				SchemaData: nil,
			}
			sc.SchemaData, _ = json.Marshal(hindex)

			// check if we need propose the updated index state
			localHIndex, ok := schemaMap[hindex.Name]
			if !ok {
				if hindex.State == common.DeletedIndex {
					continue
				}
				if hindex.State != common.InitIndex {
					cluster.CoordLog().Warningf("namespace %v update index state invalid: %v, local index not init",
						localNamespace.FullName(), hindex)
					continue
				}
				cluster.CoordLog().Infof("namespace %v init new index : %v, table: %v",
					localNamespace.FullName(), hindex, table)
				localNamespace.Node.ProposeChangeTableSchema(table, sc)
			} else if hindex.State == localHIndex.State {
			} else {
				switch hindex.State {
				case common.BuildingIndex:
					if localHIndex.State == common.InitIndex {
						cluster.CoordLog().Infof("namespace %v index start to build : %v, %v",
							localNamespace.FullName(), hindex, table)
						sc.Type = node.SchemaChangeUpdateHsetIndex
						localNamespace.Node.ProposeChangeTableSchema(table, sc)
					}
				case common.ReadyIndex:
					if localHIndex.State == common.BuildDoneIndex {
						cluster.CoordLog().Infof("namespace %v index ready for read: %v, %v",
							localNamespace.FullName(), hindex, table)
						sc.Type = node.SchemaChangeUpdateHsetIndex
						localNamespace.Node.ProposeChangeTableSchema(table, sc)
					}
				case common.InitIndex:
					// maybe rebuild, wait current done
					if localHIndex.State == common.BuildDoneIndex ||
						localHIndex.State == common.ReadyIndex ||
						localHIndex.State == common.DeletedIndex {
						cluster.CoordLog().Warningf("namespace %v rebuild index: %v for table %v, local index state: %v",
							localNamespace.FullName(), hindex, table, localHIndex)
						sc.Type = node.SchemaChangeUpdateHsetIndex
						localNamespace.Node.ProposeChangeTableSchema(table, sc)
					}
				case common.DeletedIndex:
					// remove local
					if localHIndex.State == common.BuildDoneIndex ||
						localHIndex.State == common.ReadyIndex {
						sc.Type = node.SchemaChangeDeleteHsetIndex
						localNamespace.Node.ProposeChangeTableSchema(table, sc)
					}
				default:
				}
			}
		}
		for _, jsonIndex := range tindexes.JSONIndexes {
			_ = jsonIndex
		}
	}
}

package server

import (
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/redcon"
)

func isValidPostSearchCmd(cmd string) bool {
	return cmd == "hget" || cmd == "hmget" || cmd == "hgetall"
}

func (s *Server) doMergeIndexSearch(conn redcon.Conn, cmd redcon.Command) {
	var postCmd string
	if len(cmd.Args) >= 5 {
		postCmd = string(cmd.Args[4])
		if !isValidPostSearchCmd(postCmd) {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
	}

	sLog.Debugf("secondary index query cmd: %v, %v", string(cmd.Raw), len(cmd.Args))

	result, err := s.dispatchAndWaitMergeCmd(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// TODO: maybe sort search results by field from all partitions
	postCmdResults := make([]common.HIndexRespWithValues, 0)
	pkResults := make([]rockredis.HIndexResp, 0)
	var table string
	for _, res := range result {
		if err, ok := res.(error); ok {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
		realRes, ok := res.(*node.HindexSearchResults)
		if !ok {
			sLog.Infof("invalid response for search : %v, cmd: %v", res, string(cmd.Raw))
			conn.WriteError("Invalid response type : Err handle command " + string(cmd.Args[0]))
			return
		}
		if table == "" {
			table = realRes.Table
		} else if table != realRes.Table {
			sLog.Infof("search response invalid for across table : %v, %v", table, realRes.Table)
			conn.WriteError("Invalid response table : Err handle command " + string(cmd.Args[0]))
			return
		}
		if postCmd == "" {
			pkList, ok := realRes.Rets.([]rockredis.HIndexResp)
			if !ok {
				sLog.Infof("invalid response for search : %v, cmd: %v", res, string(cmd.Raw))
				conn.WriteError("Invalid response type : Err handle command " + string(cmd.Args[0]))
				return
			}
			// only return hash keys
			pkResults = append(pkResults, pkList...)
		} else {
			realList, ok := realRes.Rets.([]common.HIndexRespWithValues)
			if !ok {
				sLog.Infof("invalid response for search : %v, cmd: %v", res, string(cmd.Raw))
				conn.WriteError("Invalid response type : Err handle command " + string(cmd.Args[0]))
				return
			}
			postCmdResults = append(postCmdResults, realList...)
		}
	}
	switch postCmd {
	case "hget", "hmget", "hgetall":
		conn.WriteArray(len(postCmdResults) * 3)
		for _, res := range postCmdResults {
			if len(res.PKey) > len(table) && string(res.PKey[:len(table)]) == table {
				conn.WriteBulk(res.PKey[len(table)+1:])
			} else {
				conn.WriteBulk(res.PKey)
			}
			conn.WriteBulk(res.IndexV)
			if len(res.HsetValues) == 0 {
				conn.WriteNull()
			} else {
				if postCmd == "hget" {
					conn.WriteBulk(res.HsetValues[0])
				} else {
					conn.WriteArray(len(res.HsetValues))
					for _, v := range res.HsetValues {
						conn.WriteBulk(v)
					}
				}
			}
		}
	case "":
		conn.WriteArray(len(pkResults) * 2)
		for _, v := range pkResults {
			if len(v.PKey) > len(table) && string(v.PKey[:len(table)]) == table {
				conn.WriteBulk(v.PKey[len(table)+1:])
			} else {
				conn.WriteBulk(v.PKey)
			}
			conn.WriteBulk(v.IndexValue)
		}
	default:
		conn.WriteError(common.ErrInvalidArgs.Error())
		return
	}
}

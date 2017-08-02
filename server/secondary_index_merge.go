package server

import (
	"github.com/absolute8511/ZanRedisDB/common"
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
	postCmdResults := make([]interface{}, 0)
	pkResults := make([][]byte, 0)
	for _, res := range result {
		if err, ok := res.(error); ok {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
		if postCmd == "" {
			pkList, ok := res.([][]byte)
			if !ok {
				sLog.Infof("invalid response for search : %v, cmd: %v", res, string(cmd.Raw))
				conn.WriteError("Invalid response type : Err handle command " + string(cmd.Args[0]))
				return
			}
			// only return hash keys
			pkResults = append(pkResults, pkList...)
		} else {
			realList, ok := res.([]interface{})
			if !ok {
				sLog.Infof("invalid response for search : %v, cmd: %v", res, string(cmd.Raw))
				conn.WriteError("Invalid response type : Err handle command " + string(cmd.Args[0]))
				return
			}
			postCmdResults = append(postCmdResults, realList...)
		}
	}
	switch postCmd {
	case "hget":
		conn.WriteArray(len(postCmdResults))
		for _, res := range postCmdResults {
			realRes, ok := res.(common.KVRecord)
			if !ok {
				conn.WriteNull()
			} else {
				conn.WriteArray(2)
				conn.WriteBulk(realRes.Key)
				conn.WriteBulk(realRes.Value)
			}
		}
	case "hmget":
		conn.WriteArray(len(postCmdResults))
		for _, res := range postCmdResults {
			realRes, ok := res.(common.KVals)
			if !ok {
				conn.WriteNull()
			} else {
				conn.WriteArray(2)
				conn.WriteBulk(realRes.PK)
				conn.WriteArray(len(realRes.Vals))
				for _, v := range realRes.Vals {
					conn.WriteBulk(v)
				}
			}
		}
	case "hgetall":
		conn.WriteArray(len(postCmdResults))
		for _, res := range postCmdResults {
			realRes, ok := res.(common.KFVals)
			if !ok {
				conn.WriteNull()
			} else {
				conn.WriteArray(2)
				conn.WriteBulk(realRes.PK)
				conn.WriteArray(len(realRes.Vals))
				for _, v := range realRes.Vals {
					if v.Err != nil {
						conn.WriteNull()
					} else {
						conn.WriteArray(2)
						conn.WriteBulk(v.Rec.Key)
						conn.WriteBulk(v.Rec.Value)
					}
				}
			}
		}
	case "":
		conn.WriteArray(len(pkResults))
		for _, v := range pkResults {
			conn.WriteBulk(v)
		}
	default:
		conn.WriteError(common.ErrInvalidArgs.Error())
		return
	}
}

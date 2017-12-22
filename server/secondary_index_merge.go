package server

import (
	"bytes"
	"strconv"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/redcon"
)

func isValidPostSearchCmd(cmd string) bool {
	return cmd == "hget" || cmd == "hmget" || cmd == "hgetall"
}

// HIDX.FROM ns:table where "field1 > 1 and field1 < 2" [LIMIT offset num] [HGET $ field2]
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
	if len(cmd.Args) < 4 {
		conn.WriteError(common.ErrInvalidArgs.Error())
		return
	}
	args := cmd.Args[4:]
	origOffset := 0
	if len(args) >= 3 && bytes.Equal(bytes.ToLower(args[0]), []byte("limit")) {
		offset, err := strconv.Atoi(string(args[1]))
		if err != nil {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		origOffset = offset
		args[1] = []byte("0")
	}
	_ = origOffset

	_, result, err := s.dispatchAndWaitMergeCmd(cmd)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	// TODO: maybe sort search results by field from all partitions
	hsetResults := make([]common.HIndexRespWithValues, 0)
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
		hsetResults = append(hsetResults, realRes.Rets...)
	}
	if postCmd != "" {
		conn.WriteArray(len(hsetResults) * 3)
	} else {
		conn.WriteArray(len(hsetResults) * 2)
	}
	for _, res := range hsetResults {
		if len(res.PKey) > len(table) && string(res.PKey[:len(table)]) == table {
			conn.WriteBulk(res.PKey[len(table)+1:])
		} else {
			conn.WriteBulk(res.PKey)
		}
		switch realV := res.IndexV.(type) {
		case []byte:
			conn.WriteBulk(realV)
		case int:
			conn.WriteInt(realV)
		case int64:
			conn.WriteInt64(realV)
		case int32:
			conn.WriteInt64(int64(realV))
		default:
			conn.WriteError("Invalid response type for index value")
		}
		if postCmd == "" {
			continue
		}
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
}

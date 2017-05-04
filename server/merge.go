package server

import (
	"encoding/base64"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

// handle range, scan command which need across multi partitions

func (self *Server) dealMergeCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) >= 2 {
		var err error
		var count int
		var countIndex int
		for i := 0; i < len(cmd.Args); i++ {
			if strings.ToLower(string(cmd.Args[i])) == "count" {
				if i+1 >= len(cmd.Args) {
					conn.WriteError(common.ErrInvalidArgs.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
				countIndex = i + 1

				count, err = strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
				break
			}
		}

		if err == nil {
			var wg sync.WaitGroup
			if err == nil {
				var results []interface{}

				cmdName := qcmdlower(cmd.Args[0])
				handlers, cmds, err := self.GetMergeHandlers(cmdName, cmd)

				if err == nil {
					length := len(handlers)
					everyCount := count / length
					results = make([]interface{}, length)
					for i, h := range handlers {
						wg.Add(1)
						cmds[i].Args[countIndex] = []byte(strconv.Itoa(everyCount))
						go func(index int, handle common.MergeCommandFunc) {
							defer wg.Add(-1)
							results[index], _ = handle(cmds[index])
						}(i, h)
					}
				} else {
					conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
				wg.Wait()

				if len(results) <= 0 {
					conn.WriteArray(2)
					conn.WriteBulkString("")

					conn.WriteArray(0)
					return
				}
				switch results[0].(type) {
				case common.ScanResult:
					self.dealScanMergeCommand(conn, results)
				}

			} else {
				conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			}
		} else {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		}
	} else {
		conn.WriteError(common.ErrInvalidArgs.Error() + " : Err handle command " + string(cmd.Args[0]))
	}
}

func (self *Server) dealScanMergeCommand(conn redcon.Conn, results []interface{}) {

	nextCursorBytes := []byte("")
	result := make([]interface{}, 0)
	for _, res := range results {
		realRes := res.(common.ScanResult)
		if realRes.Error == nil {
			v := reflect.ValueOf(realRes.Result)
			if v.Kind() != reflect.Slice {
				continue
			}

			if len(realRes.NextCursor) > 0 {
				nextCursorBytes = append(nextCursorBytes, []byte(realRes.NodeInfo)...)
				nextCursorBytes = append(nextCursorBytes, common.SCAN_NODE_SEP...)
				nextCursorBytes = append(nextCursorBytes, []byte(base64.StdEncoding.EncodeToString(realRes.NextCursor))...)
				nextCursorBytes = append(nextCursorBytes, common.SCAN_CURSOR_SEP...)
			}
			cnt := v.Len()
			for i := 0; i < cnt; i++ {
				result = append(result, v.Index(i).Interface())
			}
		} else {
			//TODO: log sth
		}
	}
	nextCursor := base64.StdEncoding.EncodeToString(nextCursorBytes)
	conn.WriteArray(2)
	conn.WriteBulkString(nextCursor)

	conn.WriteArray(len(result))
	for _, v := range result {
		conn.WriteBulk(v.([]byte))
	}

}

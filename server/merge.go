package server

import (
	"bytes"
	"encoding/base64"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/tidwall/redcon"
)

var (
	errMaxScanJob = errors.New("too much scan job")
)

var (
	scanJobCount int32 = 0
)

// handle range, scan command which need across multi partitions

func (s *Server) doMergeCommand(conn redcon.Conn, cmd redcon.Command) {
	cmdName := qcmdlower(cmd.Args[0])

	if common.IsMergeScanCommand(cmdName) {
		s.doScan(conn, cmd)
	}

}

func (s *Server) doScan(conn redcon.Conn, cmd redcon.Command) {
	if scanJobCount >= s.maxScanJob {
		conn.WriteError(errMaxScanJob.Error() + " : Err handle command " + string(cmd.Args[0]))
		return
	}
	atomic.AddInt32(&scanJobCount, 1)
	scanStart := time.Now()
	defer func(start time.Time) {
		scanCost := time.Since(scanStart)
		if scanCost >= 5*time.Second {
			sLog.Infof("slow write command: %v, cost: %v", string(cmd.Raw), scanCost)
		}
		s.scanStats.UpdateScanStats(scanCost.Nanoseconds() / 1000)
		atomic.AddInt32(&scanJobCount, -1)
	}(scanStart)

	if len(cmd.Args) >= 2 {
		var count int
		var countIndex int
		var err error
		rawKey := cmd.Args[1]

		_, _, err = common.ExtractNamesapce(rawKey)
		if err != nil {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}

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

		var wg sync.WaitGroup
		var results []interface{}
		handlers, cmds, err := s.GetMergeHandlers(cmd)

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
					nextCursorBytes = append(nextCursorBytes, []byte(realRes.PartionId)...)
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

	} else {
		conn.WriteError(common.ErrInvalidArgs.Error() + " : Err handle command " + string(cmd.Args[0]))
	}
}

func (s *Server) doScanNodesFilter(key []byte, namespace string, cmd redcon.Command, nodes map[string]*node.NamespaceNode) (map[string]redcon.Command, error) {
	cmds := make(map[string]redcon.Command)
	nsMap, err := s.decodeCursor(key, namespace)
	if err != nil {
		return nil, err
	}
	if len(nsMap) == 0 {
		for k, _ := range nodes {
			newCmd := common.DeepCopyCmd(cmd)
			cmds[k] = newCmd
		}
		return cmds, nil
	}
	for k, _ := range nodes {
		if cursor, ok := nsMap[k]; !ok {
			delete(nodes, k)
		} else {
			newCmd := common.DeepCopyCmd(cmd)
			newCmd.Args[1] = []byte(namespace + ":" + cursor)
			cmds[k] = newCmd
		}
	}
	return cmds, nil
}

func (s *Server) decodeCursor(key []byte, nsBaseName string) (map[string]string, error) {

	//key = table:cursor
	originCursor := bytes.Split(key, common.SCAN_NODE_SEP)

	//key:namespace val:new cursor
	nsMap := make(map[string]string)

	if len(originCursor) == 2 {
		table := string(originCursor[0])
		encodedCursors := originCursor[1]
		if len(table) > 0 {
			if len(encodedCursors) == 0 {
				return nsMap, nil
			}

			decodedCursors, err := base64.StdEncoding.DecodeString(string(encodedCursors))
			if err == nil {
				decodedCursors = bytes.TrimRight(decodedCursors, string(common.SCAN_CURSOR_SEP))
				cursors := bytes.Split(decodedCursors, common.SCAN_CURSOR_SEP)
				if len(cursors) > 0 {
					for _, c := range cursors {
						cursorinfo := bytes.Split(c, common.SCAN_NODE_SEP)
						if len(cursorinfo) == 2 {
							cursorEncoded := cursorinfo[1]
							cursorDecoded, err := base64.StdEncoding.DecodeString(string(cursorEncoded))
							if err == nil {
								splits := bytes.SplitN(cursorDecoded, []byte(":"), 2)
								tab := string(splits[0])
								if table != tab {
									return nil, common.ErrInvalidScanCursor
								}
								pid, err := strconv.Atoi(string(cursorinfo[0]))
								if err != nil {
									return nil, common.ErrInvalidScanCursor
								}
								ns := common.GetNsDesp(nsBaseName, pid)
								nsMap[ns] = string(cursorDecoded)
							} else {
								return nil, common.ErrInvalidScanCursor
							}

						} else {
							return nil, common.ErrInvalidScanCursor
						}
					}
				} else {
					return nil, common.ErrInvalidScanCursor
				}
			} else {
				return nil, common.ErrInvalidScanCursor
			}
		} else {
			return nil, common.ErrScanCursorNoTable
		}
	} else {
		return nil, common.ErrInvalidScanCursor
	}
	return nsMap, nil
}

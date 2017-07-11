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
	"github.com/absolute8511/redcon"
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
		if common.IsFullScanCommand(cmdName) {
			s.doMergeFullScan(conn, cmd)
		} else {
			s.doMergeScan(conn, cmd)
		}
	}

}

func (s *Server) doScanCommon(cmd redcon.Command) ([]interface{}, error) {
	if scanJobCount >= s.maxScanJob {
		return nil, errMaxScanJob
	}
	atomic.AddInt32(&scanJobCount, 1)
	scanStart := time.Now()
	defer func(start time.Time) {
		scanCost := time.Since(scanStart)
		if scanCost >= 5*time.Second {
			sLog.Infof("slow merge command: %v, cost: %v", string(cmd.Raw), scanCost)
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
			return nil, err
		}

		for i := 0; i < len(cmd.Args); i++ {
			if strings.ToLower(string(cmd.Args[i])) == "count" {
				if i+1 >= len(cmd.Args) {
					return nil, common.ErrInvalidArgs
				}
				countIndex = i + 1

				count, err = strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					return nil, err
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
					defer wg.Done()
					results[index], _ = handle(cmds[index])
				}(i, h)
			}
		} else {
			return nil, err
		}
		wg.Wait()

		return results, nil
	} else {
		return nil, common.ErrInvalidArgs
	}
}

func (s *Server) doMergeFullScan(conn redcon.Conn, cmd redcon.Command) {
	results, err := s.doScanCommon(cmd)
	if err != nil {
		conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		return
	}

	nextCursorBytes := []byte("")
	var dataType common.DataType
	var count int
	for _, res := range results {
		realRes := res.(*common.FullScanResult)
		if realRes.Error == nil {
			dataType = realRes.Type

			if len(realRes.NextCursor) > 0 {
				nextCursorBytes = append(nextCursorBytes, []byte(realRes.PartionId)...)
				nextCursorBytes = append(nextCursorBytes, common.SCAN_NODE_SEP...)

				nextCursorBytes = append(nextCursorBytes, []byte(base64.StdEncoding.EncodeToString(realRes.NextCursor))...)
				nextCursorBytes = append(nextCursorBytes, common.SCAN_CURSOR_SEP...)
			}
			count += len(realRes.Results)
		} else {
			conn.WriteError(realRes.Error.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
	}

	nextCursor := base64.StdEncoding.EncodeToString(nextCursorBytes)
	conn.WriteArray(2)
	conn.WriteBulkString(nextCursor)

	switch dataType {
	case common.KV:
		conn.WriteArray(count)
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			//此处可以不用检查了
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					conn.WriteArray(2)
					conn.WriteBulk(r.([][]byte)[0])
					conn.WriteBulk(r.([][]byte)[1])
				}
			}
		}
	case common.HASH:
		conn.WriteArray(count)
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					conn.WriteArray(3)
					realR := r.([][]byte)
					length := len(realR)
					if length == 3 {
						conn.WriteBulk(realR[0])
						conn.WriteBulk(realR[1])
						conn.WriteBulk(realR[2])
					}
				}
			}
		}
	case common.LIST, common.SET:
		conn.WriteArray(count)
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					realR := r.([][]byte)
					length := len(realR)
					conn.WriteArray(length)
					for idx, _ := range realR {
						conn.WriteBulk(realR[idx])
					}
				}
			}
		}

		/*
			case common.ZSET:
				conn.WriteArray(len(result) * 2)
				for k, v := range result {
					conn.WriteBulkString(k)
					val := v.([]common.ScorePair)
					conn.WriteArray(len(val) * 2)
					for _, s := range val {
						conn.WriteBulk(s.Member)
						conn.WriteBulkString(strconv.FormatInt(s.Score, 10))
					}
				}
		*/
	}

}

func (s *Server) doMergeScan(conn redcon.Conn, cmd redcon.Command) {
	results, err := s.doScanCommon(cmd)
	if err != nil {
		conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		return
	}

	nextCursorBytes := []byte("")
	result := make([]interface{}, 0)
	for _, res := range results {
		realRes := res.(*common.ScanResult)
		if realRes.Error == nil {
			v := reflect.ValueOf(realRes.Keys)
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

//首次传入 namespace:table:,
//返回 1:xxx1;2:xxx2;3:xxx3,
//下次传入 namespace:table:1:xxx;2:xxx;3:xxx,
//解析出分区 1, 2, 3 及其对应的cursor,
//1的namespace:table:xxx1和2的namespace:table:xxx2, 和3的namespace:table:xxx3,
func (s *Server) decodeCursor(key []byte, nsBaseName string) (map[string]string, error) {

	//key = table:cursor
	originCursor := bytes.Split(key, common.SCAN_NODE_SEP)

	//key:namespace val:new cursor
	nsMap := make(map[string]string)

	if len(originCursor) == 2 {
		table := originCursor[0]
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
								pid, err := strconv.Atoi(string(cursorinfo[0]))
								if err != nil {
									return nil, common.ErrInvalidScanCursor
								}
								ns := common.GetNsDesp(nsBaseName, pid)
								var cursor []byte
								cursor = append(cursor, table...)
								cursor = append(cursor, common.SCAN_NODE_SEP...)
								cursor = append(cursor, cursorDecoded...)
								nsMap[ns] = string(cursor)
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

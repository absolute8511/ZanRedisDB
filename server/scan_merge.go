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

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/node"
	"github.com/absolute8511/redcon"
)

var (
	errMaxScanJob = errors.New("too much scan job")
)

var (
	scanJobCount int32
)

// handle range, scan command which need across multi partitions
func (s *Server) doScanCommon(cmd redcon.Command) ([]interface{}, []byte, error) {
	if atomic.LoadInt32(&scanJobCount) >= s.maxScanJob {
		return nil, nil, errMaxScanJob
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

		_, rk, err := common.ExtractNamesapce(rawKey)
		if err != nil {
			return nil, nil, err
		}
		table, _, err := common.ExtractTable(rk)
		if err != nil {
			return nil, nil, err
		}

		for i := 0; i < len(cmd.Args); i++ {
			if strings.ToLower(string(cmd.Args[i])) == "count" {
				if i+1 >= len(cmd.Args) {
					return nil, nil, common.ErrInvalidArgs
				}
				countIndex = i + 1

				count, err = strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					return nil, nil, err
				}
				break
			}
		}

		var wg sync.WaitGroup
		var results []interface{}
		handlers, cmds, _, err := s.GetMergeHandlers(cmd)
		if err == nil {
			length := len(handlers)
			everyCount := count / length
			results = make([]interface{}, length)
			for i, h := range handlers {
				wg.Add(1)
				cmds[i].Args[countIndex] = []byte(strconv.Itoa(everyCount))
				go func(index int, handle common.MergeCommandFunc) {
					defer wg.Done()
					var err error
					results[index], err = handle(cmds[index])
					if err != nil {
						results[index] = err
					}
				}(i, h)
			}
		} else {
			return nil, nil, err
		}
		wg.Wait()

		return results, table, nil
	} else {
		return nil, nil, common.ErrInvalidArgs
	}
}

func (s *Server) doMergeFullScan(conn redcon.Conn, cmd redcon.Command) {
	results, table, err := s.doScanCommon(cmd)
	if err != nil {
		conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		return
	}

	nextCursorBytes := []byte("")
	var dataType common.DataType
	var count int
	for _, res := range results {
		if err, ok := res.(error); ok {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
		realRes, ok := res.(*common.FullScanResult)
		if !ok {
			conn.WriteError("response type invalid : Err handle command " + string(cmd.Args[0]))
			return
		} else if realRes.Error != nil {
			conn.WriteError(realRes.Error.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
		dataType = realRes.Type
		if len(realRes.NextCursor) > 0 {
			nextCursorBytes = append(nextCursorBytes, []byte(realRes.PartionId)...)
			nextCursorBytes = append(nextCursorBytes, common.SCAN_NODE_SEP...)

			nextCursorBytes = append(nextCursorBytes, []byte(base64.StdEncoding.EncodeToString(realRes.NextCursor))...)
			nextCursorBytes = append(nextCursorBytes, common.SCAN_CURSOR_SEP...)
		}
		count += len(realRes.Results)
	}

	nextCursor := base64.StdEncoding.EncodeToString(nextCursorBytes)
	conn.WriteArray(2)
	conn.WriteBulkString(nextCursor)
	conn.WriteArray(count)

	tabLen := len(table)

	switch dataType {
	case common.KV:
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					realR := r.([]interface{})
					length := len(realR)
					conn.WriteArray(length)
					k := realR[0].([]byte)
					v := realR[1].([]byte)
					conn.WriteBulk(k[tabLen+1:])
					conn.WriteBulk(v)
				}
			}
		}
	case common.HASH:
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					realR := r.([]interface{})
					length := len(realR)
					conn.WriteArray(length)
					k := realR[0].([]byte)
					conn.WriteBulk(k)
					for i := 1; i < length; i++ {
						v := realR[i].(common.FieldPair)
						conn.WriteArray(2)
						conn.WriteBulk(v.Field)
						conn.WriteBulk(v.Value)
					}
				}
			}
		}
	case common.LIST, common.SET:
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					realR := r.([]interface{})
					length := len(realR)
					conn.WriteArray(length)
					for idx := range realR {
						v := realR[idx].([]byte)
						conn.WriteBulk(v)
					}
				}
			}
		}
	case common.ZSET:
		for _, res := range results {
			realRes := res.(*common.FullScanResult)
			if realRes.Error == nil {
				for _, r := range realRes.Results {
					realR := r.([]interface{})
					length := len(realR)
					conn.WriteArray(length)
					k := realR[0].([]byte)
					conn.WriteBulk(k)
					for i := 1; i < length; i++ {
						v := realR[i].(common.ScorePair)
						conn.WriteArray(2)
						conn.WriteBulk(v.Member)
						conn.WriteBulk([]byte(strconv.FormatFloat(v.Score, 'g', -1, 64)))
					}
				}
			}
		}
	}
}

func (s *Server) doMergeScan(conn redcon.Conn, cmd redcon.Command) {
	results, table, err := s.doScanCommon(cmd)
	if err != nil {
		conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		return
	}

	nextCursorBytes := []byte("")
	result := make([]interface{}, 0, len(results))
	for _, res := range results {
		if err, ok := res.(error); ok {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}
		realRes, ok := res.(*common.ScanResult)
		if !ok {
			conn.WriteError("response type invalid : Err handle command " + string(cmd.Args[0]))
			return
		} else if realRes.Error != nil {
			conn.WriteError(realRes.Error.Error() + " : Err handle command " + string(cmd.Args[0]))
			return
		}

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
	}

	nextCursor := base64.StdEncoding.EncodeToString(nextCursorBytes)
	conn.WriteArray(2)
	conn.WriteBulkString(nextCursor)

	conn.WriteArray(len(result))
	tabLen := len(table)
	for _, v := range result {
		conn.WriteBulk(v.([]byte)[tabLen+1:])
	}

}

func (s *Server) doScanNodesFilter(key []byte, namespace string, cmd redcon.Command, nodes map[string]*node.NamespaceNode) (map[string]redcon.Command, error) {
	cmds := make(map[string]redcon.Command)
	nsMap, err := s.decodeScanCursor(key, namespace)
	if err != nil {
		return nil, err
	}
	if len(nsMap) == 0 {
		for k := range nodes {
			newCmd := common.DeepCopyCmd(cmd)
			cmds[k] = newCmd
		}
		return cmds, nil
	}
	for k := range nodes {
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
func (s *Server) decodeScanCursor(key []byte, nsBaseName string) (map[string]string, error) {

	//key = table:cursor
	originCursor := bytes.Split(key, common.SCAN_NODE_SEP)

	//key:namespace val:new cursor
	nsMap := make(map[string]string)
	if len(originCursor) != 2 {
		return nil, common.ErrInvalidScanCursor
	}
	table := originCursor[0]
	if len(table) <= 0 {
		return nil, common.ErrScanCursorNoTable
	}
	encodedCursors := originCursor[1]
	if len(encodedCursors) == 0 {
		return nsMap, nil
	}
	decodedCursors, err := base64.StdEncoding.DecodeString(string(encodedCursors))
	if err != nil {
		return nil, common.ErrInvalidScanCursor
	}
	decodedCursors = bytes.TrimRight(decodedCursors, string(common.SCAN_CURSOR_SEP))
	cursors := bytes.Split(decodedCursors, common.SCAN_CURSOR_SEP)
	if len(cursors) <= 0 {
		return nil, common.ErrInvalidScanCursor
	}
	for _, c := range cursors {
		cursorinfo := bytes.Split(c, common.SCAN_NODE_SEP)
		if len(cursorinfo) != 2 {
			return nil, common.ErrInvalidScanCursor
		}
		cursorEncoded := cursorinfo[1]
		cursorDecoded, err := base64.StdEncoding.DecodeString(string(cursorEncoded))
		if err != nil {
			return nil, common.ErrInvalidScanCursor
		}
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
	}

	return nsMap, nil
}

package node

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

func parseScanArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
	if len(args) == 0 {
		return
	}
	cursor = args[0]
	args = args[1:]
	count = 0

	for i := 0; i < len(args); {
		switch strings.ToLower(string(args[i])) {
		case "match":
			if i+1 >= len(args) {
				err = common.ErrInvalidArgs
				return
			}
			match = string(args[i+1])
			i++
		case "count":
			if i+1 >= len(args) {
				err = common.ErrInvalidArgs
				return
			}

			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return
			}

			i++
		default:
			err = fmt.Errorf("invalid argument %s", args[i])
			return
		}

		i++
	}
	return
}

// SCAN cursor [MATCH match] [COUNT count]
// scan only kv type, cursor is table:key

// TODO: for scan we act like the prefix scan, if the prefix changed , we should stop scan
func (self *KVNode) scanCommand(cmd redcon.Command) (interface{}, error) {
	args := cmd.Args[1:]
	cursor, match, count, err := parseScanArgs(args)

	if err != nil {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}

	ay, err := self.store.Scan(common.KV, cursor, count, match)
	if err != nil {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}

	var nextCursor []byte
	if len(ay) < count || (count == 0 && len(ay) == 0) {
		nextCursor = []byte("")
	} else {
		nextCursor = ay[len(ay)-1]
	}
	_, pid := common.GetNamespaceAndPartition(self.ns)
	return common.ScanResult{Result: ay, NextCursor: nextCursor, PartionId: strconv.Itoa(pid), Error: nil}, nil
}

// ADVSCAN cursor type [MATCH match] [COUNT count]
// here cursor is the scan key for start, (table:key)
// and the response will return the next start key for next scan,
// (note: it is not the "0" as the redis scan to indicate the end of scan)
func (self *KVNode) advanceScanCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 3 {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: common.ErrInvalidArgs}, common.ErrInvalidArgs
	}

	var dataType common.DataType
	switch strings.ToUpper(string(cmd.Args[2])) {
	case "KV":
		dataType = common.KV
	case "HASH":
		dataType = common.HASH
	case "LIST":
		dataType = common.LIST
	case "SET":
		dataType = common.SET
	case "ZSET":
		dataType = common.ZSET
	default:
		return common.ScanResult{Result: nil, NextCursor: nil, Error: common.ErrInvalidScanType}, common.ErrInvalidScanType
	}
	_, key, err := common.ExtractNamesapce(cmd.Args[1])
	if err != nil {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}
	cmd.Args[1] = key
	cmd.Args[1], cmd.Args[2] = cmd.Args[2], cmd.Args[1]

	cursor, match, count, err := parseScanArgs(cmd.Args[2:])

	if err != nil {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}

	var ay [][]byte

	ay, err = self.store.Scan(dataType, cursor, count, match)

	if err != nil {
		return common.ScanResult{Result: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}

	var nextCursor []byte
	if len(ay) < count || (count == 0 && len(ay) == 0) {
		nextCursor = []byte("")
	} else {
		nextCursor = ay[len(ay)-1]
	}
	_, pid := common.GetNamespaceAndPartition(self.ns)
	return common.ScanResult{Result: ay, NextCursor: nextCursor, PartionId: strconv.Itoa(pid), Error: nil}, nil
}

// HSCAN key cursor [MATCH match] [COUNT count]
// key is (table:key)
func (self *KVNode) hscanCommand(conn redcon.Conn, cmd redcon.Command) {
	// the cursor can be nil means scan from start of the hash
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	args := cmd.Args[1:]
	key := args[0]
	cursor, match, count, err := parseScanArgs(args[1:])

	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	var ay []common.KVRecord

	ay, err = self.store.HScan(key, cursor, count, match)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	var nextCursor []byte
	if len(ay) < count || (count == 0 && len(ay) == 0) {
		nextCursor = []byte("")
	} else {
		nextCursor = ay[len(ay)-1].Key
	}

	conn.WriteArray(2)
	conn.WriteBulk(nextCursor)
	conn.WriteArray(len(ay) * 2)
	for _, v := range ay {
		conn.WriteBulk(v.Key)
		conn.WriteBulk(v.Value)
	}
	return
}

//SSCAN key cursor [MATCH match] [COUNT count]
// key is (table:key)
func (self *KVNode) sscanCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	args := cmd.Args[1:]
	key := args[0]

	cursor, match, count, err := parseScanArgs(args[1:])

	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	var ay [][]byte
	ay, err = self.store.SScan(key, cursor, count, match)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	var nextCursor []byte
	if len(ay) < count || (count == 0 && len(ay) == 0) {
		nextCursor = []byte("")
	} else {
		nextCursor = ay[len(ay)-1]
	}

	conn.WriteArray(2)
	conn.WriteBulk(nextCursor)
	conn.WriteArray(len(ay))
	for _, v := range ay {
		conn.WriteBulk(v)
	}
}

// ZSCAN key cursor [MATCH match] [COUNT count]
// key is (table:key)
func (self *KVNode) zscanCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	args := cmd.Args[1:]
	key := args[0]

	cursor, match, count, err := parseScanArgs(args[1:])

	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	var ay []common.ScorePair

	ay, err = self.store.ZScan(key, cursor, count, match)

	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	vv := make([][]byte, 0, len(ay)*2)

	for _, v := range ay {
		vv = append(vv, v.Member)
	}

	var nextCursor []byte
	if len(ay) < count || (count == 0 && len(ay) == 0) {
		nextCursor = []byte("")
	} else {
		nextCursor = ay[len(ay)-1].Member
	}

	conn.WriteArray(2)
	conn.WriteBulk(nextCursor)
	conn.WriteArray(len(ay) * 2)
	for _, v := range ay {
		conn.WriteBulk(v.Member)
		conn.WriteBulk([]byte(strconv.FormatInt(v.Score, 10)))
	}
	return
}

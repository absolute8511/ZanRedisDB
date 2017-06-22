package node

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
)

func parseBackupArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
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

// backup cursor type [MATCH match] [COUNT count]
// here cursor is the scan key for start, (table:key)
// and the response will return the next start key for next scan,
// (note: it is not the "0" as the redis scan to indicate the end of scan)
func (self *KVNode) backupCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 3 {
		return &common.ScanResult{Keys: nil, Values: nil, NextCursor: nil, PartionId: "", Error: common.ErrInvalidArgs}, common.ErrInvalidArgs
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
		return &common.ScanResult{Keys: nil, Values: nil, NextCursor: nil, Error: common.ErrInvalidScanType}, common.ErrInvalidScanType
	}
	_, key, err := common.ExtractNamesapce(cmd.Args[1])
	if err != nil {
		return &common.ScanResult{Keys: nil, Values: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}
	cmd.Args[1] = key
	cmd.Args[1], cmd.Args[2] = cmd.Args[2], cmd.Args[1]

	cursor, match, count, err := parseScanArgs(cmd.Args[2:])
	if err != nil {
		return &common.ScanResult{Keys: nil, Values: nil, NextCursor: nil, PartionId: "", Error: err}, err
	}

	splits := bytes.SplitN(cursor, []byte(":"), 2)
	if len(splits) != 2 {
		return &common.ScanResult{Keys: nil, Values: nil, NextCursor: nil, PartionId: "", Error: common.ErrInvalidScanCursor}, common.ErrInvalidScanCursor
	}

	table := splits[0]

	result := self.store.AdvanceScan(dataType, cursor, count, match)
	result.Type = dataType

	if result.Error != nil {
		return result, result.Error
	}

	length := len(result.Keys)
	if length < count || (count == 0 && length == 0) {
		result.NextCursor = []byte("")
	} else {
		result.NextCursor = result.Keys[length-1]
	}

	if length > 0 {
		item := result.Keys[length-1]
		tab, _, err := common.ExtraTable(item)
		if err == nil && !bytes.Equal(tab, table) {
			result.NextCursor = []byte("")
			for _, v := range result.Keys {
				tab, _, err := common.ExtraTable(v)
				if err != nil || !bytes.Equal(tab, table) {
					delete(result.Values, string(v))
				}
			}
		}
	}
	_, pid := common.GetNamespaceAndPartition(self.ns)
	result.PartionId = strconv.Itoa(pid)
	return result, nil
}

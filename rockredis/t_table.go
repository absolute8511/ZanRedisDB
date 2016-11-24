package rockredis

import (
	"errors"
)

var (
	errTableNameLen = errors.New("invalid table name length")
)

const (
	tableStartSep byte = ':'
	tableStopSep  byte = tableStartSep + 1
	colStartSep   byte = ':'
	colStopSep    byte = colStartSep + 1
)

// only KV data type need a table name as prefix to allow scan by table
func checkTableName(table string) error {
	if len(table) >= MaxTableNameLen || len(table) == 0 {
		return errTableNameLen
	}
	return nil
}

func getTableEncodeLen(table string) int {
	return 1 + len(table)
}

func encodeTableName(table string, buf []byte) []byte {
	tlen := byte(len(table))
	buf[0] = tlen
	copy(buf[1:], []byte(table))
	return buf
}

func decodeTableName(d []byte) (string, error) {
	tlen := int(d[0])
	if tlen >= MaxTableNameLen {
		return "", errTableNameLen
	}
	table := string(d[1 : tlen+1])
	return table, nil
}

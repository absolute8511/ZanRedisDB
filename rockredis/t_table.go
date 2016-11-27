package rockredis

import (
	"errors"
)

var (
	errTableNameLen = errors.New("invalid table name length")
	errTableName    = errors.New("invalid table name")
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
	return 1 + len(table) + 1
}

func encodeTableName(table string, buf []byte) []byte {
	tlen := byte(len(table))
	pos := 0
	buf[pos] = tlen
	pos++
	copy(buf[pos:], []byte(table))
	pos += len(table)
	buf[pos] = tableStartSep
	return buf
}

func decodeTableName(d []byte) (string, []byte, error) {
	pos := 0
	tlen := int(d[0])
	if tlen >= MaxTableNameLen {
		return "", d, errTableNameLen
	}
	pos++
	table := string(d[pos : pos+tlen])
	pos += tlen
	if d[pos] != tableStartSep {
		return "", d, errTableName
	}
	pos++
	d = d[pos:]
	return table, d, nil
}

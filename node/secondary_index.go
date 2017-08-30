package node

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/redcon"
)

type HindexSearchResults struct {
	Table string
	Rets  []common.HIndexRespWithValues
}

func parseSingleCond(condData []byte, indexCond *rockredis.IndexCondition) ([]byte, error) {
	condData = bytes.TrimSpace(condData)
	var field []byte
	if pos := bytes.Index(condData, []byte("=")); pos != -1 {
		if condData[pos-1] == '<' {
			indexCond.EndKey = condData[pos+1:]
			indexCond.IncludeEnd = true
			field = condData[:pos-1]
		} else if condData[pos-1] == '>' {
			indexCond.StartKey = condData[pos+1:]
			indexCond.IncludeStart = true
			field = condData[:pos-1]
		} else {
			indexCond.StartKey = condData[pos+1:]
			indexCond.IncludeStart = true
			indexCond.EndKey = condData[pos+1:]
			indexCond.IncludeEnd = true
			field = condData[:pos]
		}
	} else if pos = bytes.Index(condData, []byte(">")); pos != -1 {
		indexCond.StartKey = condData[pos+1:]
		indexCond.IncludeStart = false
		field = condData[:pos]
	} else if pos = bytes.Index(condData, []byte("<")); pos != -1 {
		indexCond.EndKey = condData[pos+1:]
		indexCond.IncludeEnd = false
		field = condData[:pos]
	} else {
		return nil, common.ErrInvalidArgs
	}

	return field, nil
}

// TODO: handle string index value which may contains ">, =, <, and" words
func parseIndexQueryWhere(whereData []byte) ([]byte, *rockredis.IndexCondition, error) {
	whereData = bytes.Trim(whereData, "\"")
	andConds := bytes.SplitN(whereData, []byte("and"), 2)
	if len(andConds) != 1 && len(andConds) != 2 {
		return nil, nil, common.ErrInvalidArgs
	}
	indexCond := &rockredis.IndexCondition{
		Offset: 0,
		Limit:  -1,
	}
	field, err := parseSingleCond(andConds[0], indexCond)
	if err != nil {
		return nil, nil, err
	}
	if len(andConds) == 2 {
		field2, err := parseSingleCond(andConds[1], indexCond)
		if err != nil {
			return nil, nil, err
		}
		if !bytes.Equal(field, field2) {
			return nil, nil, common.ErrInvalidArgs
		}
	}
	return field, indexCond, nil
}

func parseIndexQueryLimit(args [][]byte) (int, int, error) {
	if len(args) < 3 || strings.ToLower(string(args[0])) != "limit" {
		return 0, 0, common.ErrInvalidArgs
	}
	var offset int
	var count int
	var err error
	if offset, err = strconv.Atoi(string(args[1])); err != nil {
		return 0, 0, common.ErrInvalidArgs
	}
	if count, err = strconv.Atoi(string(args[2])); err != nil {
		return 0, 0, common.ErrInvalidArgs
	}
	return offset, count, nil
}

// HIDX.FROM ns:table where "field1 > 1 and field1 < 2" [LIMIT offset num] [HGET $ field2]
// HIDX.FROM ns:table where "field1 > 1 and field1 < 2" [LIMIT offset num] HGETALL $
// HIDX.FROM {namespace:table} WHERE {WHERE clause} [LIMIT offset num] [ANY HASH REDIS COMMAND]
// handle (xx and xx) or (xx and xx), get the min and max range and iterator in the possible range,
// match all the conditions to decide whether the cursor should be returned
func (nd *KVNode) hindexSearchCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 4 {
		return nil, common.ErrInvalidArgs
	}
	_, table, err := common.ExtractNamesapce(cmd.Args[1])
	if err != nil {
		return nil, err
	}
	cmd.Args[1] = table

	if strings.ToLower(string(cmd.Args[2])) != "where" {
		return nil, common.ErrInvalidArgs
	}
	nd.rn.Debugf("parsing where condition: %v", string(cmd.Args[3]))
	field, cond, err := parseIndexQueryWhere(cmd.Args[3])
	if err != nil {
		return nil, err
	}
	args := cmd.Args[4:]
	if len(args) >= 3 && bytes.Equal(bytes.ToLower(args[0]), []byte("limit")) {
		offset, count, err := parseIndexQueryLimit(args)
		if err != nil {
			return nil, err
		}
		cond.Offset = offset
		cond.Limit = count
		args = args[3:]
	}
	nd.rn.Debugf("table %v parsing where condition result: %v, field: %v", string(table), cond, string(field))
	vt, _, pkList, err := nd.store.HsetIndexSearch(table, field, cond, false)
	if err != nil {
		nd.rn.Infof("search %v, %v error: %v", string(table), string(field), err)
		return nil, err
	}
	nd.rn.Debugf("search result count: %v", len(pkList))
	rets := make([]common.HIndexRespWithValues, 0, len(pkList))
	if len(args) > 0 {
		postCmdArgs := args
		if len(postCmdArgs) < 2 {
			return nil, common.ErrInvalidArgs
		}
		cmdName := string(postCmdArgs[0])
		switch cmdName {
		case "hget":
			if len(postCmdArgs) < 3 {
				return nil, common.ErrInvalidArgs
			}
			for _, pk := range pkList {
				v, err := nd.store.HGet(pk.PKey, postCmdArgs[2])
				if err != nil {
					continue
				}
				vv := [][]byte{v}
				rspV := common.HIndexRespWithValues{PKey: pk.PKey, IndexV: pk.IndexValue, HsetValues: vv}
				if vt == rockredis.Int64V || vt == rockredis.Int32V {
					rspV.IndexV = pk.IndexIntValue
				}
				rets = append(rets, rspV)
			}
		case "hmget":
			if len(postCmdArgs) < 3 {
				return nil, common.ErrInvalidArgs
			}
			for _, pk := range pkList {
				vals, err := nd.store.HMget(pk.PKey, postCmdArgs[2:]...)
				if err != nil {
					continue
				}
				rspV := common.HIndexRespWithValues{PKey: pk.PKey, IndexV: pk.IndexValue, HsetValues: vals}
				if vt == rockredis.Int64V || vt == rockredis.Int32V {
					rspV.IndexV = pk.IndexIntValue
				}
				rets = append(rets, rspV)
			}
		case "hgetall":
			for _, pk := range pkList {
				_, valCh, err := nd.store.HGetAll(pk.PKey)
				if err != nil {
					continue
				}
				vv := [][]byte{}
				for v := range valCh {
					vv = append(vv, v.Rec.Key, v.Rec.Value)
				}
				rspV := common.HIndexRespWithValues{PKey: pk.PKey, IndexV: pk.IndexValue, HsetValues: vv}
				if vt == rockredis.Int64V || vt == rockredis.Int32V {
					rspV.IndexV = pk.IndexIntValue
				}
				rets = append(rets, rspV)
			}
		default:
			return nil, common.ErrNotSupport
		}
		return &HindexSearchResults{Table: string(table), Rets: rets}, nil
	} else {
		for _, pk := range pkList {
			rspV := common.HIndexRespWithValues{PKey: pk.PKey, IndexV: pk.IndexValue}
			if vt == rockredis.Int64V || vt == rockredis.Int32V {
				rspV.IndexV = pk.IndexIntValue
			}
			rets = append(rets, rspV)
		}
		return &HindexSearchResults{Table: string(table), Rets: rets}, nil
	}
}

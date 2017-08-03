package node

import (
	"bytes"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
	"github.com/absolute8511/redcon"
	"strconv"
	"strings"
)

type HindexSearchResults struct {
	Table string
	Rets  interface{}
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
func (self *KVNode) hindexSearchCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 4 {
		return nil, common.ErrInvalidArgs
	}
	_, table, err := common.ExtractNamesapce(cmd.Args[1])
	if err != nil {
		return nil, err
	}
	cmd.Args[1] = table

	self.rn.Debugf("parsing where condition: %v", string(cmd.Args[3]))
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
	self.rn.Debugf("table %v parsing where condition result: %v, field: %v", string(table), cond, string(field))
	_, pkList, err := self.store.HsetIndexSearch(table, field, cond, false)
	if err != nil {
		self.rn.Infof("search %v, %v error: %v", string(table), string(field), err)
		return nil, err
	}
	self.rn.Debugf("search result count: %v", len(pkList))
	if len(args) > 0 {
		postCmdArgs := args
		if len(postCmdArgs) < 2 {
			return nil, common.ErrInvalidArgs
		}
		rets := make([]interface{}, 0, len(pkList))
		cmdName := string(postCmdArgs[0])
		switch cmdName {
		case "hget":
			if len(postCmdArgs) < 3 {
				return nil, common.ErrInvalidArgs
			}
			for _, pk := range pkList {
				v, err := self.store.HGet(pk, postCmdArgs[2])
				if err != nil {
					continue
				}
				rets = append(rets, common.KVRecord{pk, v})
			}
		case "hmget":
			if len(postCmdArgs) < 3 {
				return nil, common.ErrInvalidArgs
			}
			for _, pk := range pkList {
				vals, err := self.store.HMget(pk, postCmdArgs[2:]...)
				if err != nil {
					continue
				}
				rets = append(rets, common.KVals{pk, vals})
			}
		case "hgetall":
			for _, pk := range pkList {
				n, valCh, err := self.store.HGetAll(pk)
				if err != nil {
					continue
				}
				fvs := make([]common.KVRecordRet, 0, n)
				for v := range valCh {
					fvs = append(fvs, v)
				}
				rets = append(rets, common.KFVals{pk, fvs})
			}
		default:
			return nil, common.ErrNotSupport
		}
		return &HindexSearchResults{Table: string(table), Rets: rets}, nil
	} else {
		return &HindexSearchResults{Table: string(table), Rets: pkList}, nil
	}
}

package node

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
)

var (
	errInvalidRange = errors.New("Invalid range string")
)

func getScoreRange(left []byte, right []byte) (float64, float64, error) {
	if len(left) == 0 || len(right) == 0 {
		return 0, 0, errInvalidRange
	}
	var leftRange float64
	var rightRange float64
	var err error
	isLOpen := false
	rangeD := left
	if strings.ToLower(string(rangeD)) == "-inf" {
		leftRange = common.MinScore
	} else {
		if left[0] == '(' {
			isLOpen = true
			rangeD = left[1:]
		}
		leftRange, err = strconv.ParseFloat(string(rangeD), 64)
		if err != nil {
			return leftRange, rightRange, err
		}
		if leftRange <= common.MinScore || leftRange >= common.MaxScore {
			return leftRange, rightRange, errInvalidRange
		}
		if isLOpen {
			leftRange++
		}
	}
	rangeD = right
	isROpen := false
	if strings.ToLower(string(rangeD)) == "+inf" {
		rightRange = common.MaxScore
	} else {
		if right[0] == '(' {
			isROpen = true
			rangeD = right[1:]
		}
		rightRange, err = strconv.ParseFloat(string(rangeD), 64)
		if err != nil {
			return leftRange, rightRange, err
		}
		if rightRange <= common.MinScore || rightRange >= common.MaxScore {
			return leftRange, rightRange, errInvalidRange
		}
		if isROpen {
			rightRange--
		}

	}
	return leftRange, rightRange, err
}

func getLexRange(left []byte, right []byte) ([]byte, []byte, uint8, error) {
	if len(left) == 0 || len(right) == 0 {
		return nil, nil, 0, errInvalidRange
	}
	var err error
	rangeType := common.RangeClose
	isLOpen := false
	if bytes.Equal(left, []byte("-")) {
		left = nil
	} else {
		if left[0] == '(' {
			isLOpen = true
			left = left[1:]
		} else if left[0] == '[' {
			isLOpen = false
			left = left[1:]
		} else {
			return left, right, rangeType, errInvalidRange
		}
	}
	isROpen := false
	if bytes.Equal(right, []byte("+")) {
		right = nil
	} else {
		if right[0] == '(' {
			isROpen = true
			right = right[1:]
		} else if right[0] == '[' {
			isROpen = false
			right = right[1:]
		} else {
			return left, right, rangeType, errInvalidRange
		}
	}
	if isLOpen && isROpen {
		rangeType = common.RangeOpen
	} else if isROpen {
		rangeType = common.RangeROpen
	} else if isLOpen {
		rangeType = common.RangeLOpen
	}
	return left, right, rangeType, err
}

func (nd *KVNode) zscoreCommand(conn redcon.Conn, cmd redcon.Command) {
	val, err := nd.store.ZScore(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk([]byte(strconv.FormatFloat(val, 'g', -1, 64)))
	}
}

func (nd *KVNode) zcountCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	min, max, err := getScoreRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	val, err := nd.store.ZCount(cmd.Args[1], min, max)
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt64(val)
	}
}

func (nd *KVNode) zcardCommand(conn redcon.Conn, cmd redcon.Command) {
	n, err := nd.store.ZCard(cmd.Args[1])
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (nd *KVNode) zlexcountCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	start, stop, rt, err := getLexRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	n, err := nd.store.ZLexCount(cmd.Args[1], start, stop, rt)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteInt64(n)
}

func (nd *KVNode) zrangeFunc(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	if len(cmd.Args) != 4 && len(cmd.Args) != 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	end, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	needScore := false
	if len(cmd.Args) == 5 {
		if strings.ToLower(string(cmd.Args[4])) == "withscores" {
			needScore = true
		} else {
			conn.WriteError(errSyntaxError.Error())
			return
		}
	}

	vlist, err := nd.store.ZRangeGeneric(cmd.Args[1], int(start), int(end), reverse)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	if needScore {
		conn.WriteArray(len(vlist) * 2)
	} else {
		conn.WriteArray(len(vlist))
	}
	for _, d := range vlist {
		conn.WriteBulk(d.Member)
		if needScore {
			conn.WriteBulkString(strconv.FormatFloat(d.Score, 'g', -1, 64))
		}
	}
}

func (nd *KVNode) zrangeCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.zrangeFunc(conn, cmd, false)
}

func (nd *KVNode) zrevrangeCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.zrangeFunc(conn, cmd, true)
}

func (nd *KVNode) zrangebylexCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 && len(cmd.Args) != 7 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	start, stop, rt, err := getLexRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError("Invalid index: " + err.Error())
		return
	}
	offset := 0
	count := -1
	if len(cmd.Args) == 7 {
		if strings.ToLower(string(cmd.Args[4])) != "limit" {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		if offset, err = strconv.Atoi(string(cmd.Args[5])); err != nil {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		if count, err = strconv.Atoi(string(cmd.Args[6])); err != nil {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
	}

	vlist, err := nd.store.ZRangeByLex(cmd.Args[1], start, stop, rt, offset, count)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	conn.WriteArray(len(vlist))
	for _, d := range vlist {
		conn.WriteBulk(d)
	}
}

func (nd *KVNode) zrangebyscoreFunc(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var min float64
	var max float64
	var err error
	if !reverse {
		min, max, err = getScoreRange(cmd.Args[2], cmd.Args[3])
	} else {
		min, max, err = getScoreRange(cmd.Args[3], cmd.Args[2])
	}
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	args := cmd.Args[4:]
	needScore := false
	if len(args) > 0 {
		if strings.ToLower(string(args[0])) == "withscores" {
			needScore = true
			args = args[1:]
		}
	}

	offset := 0
	count := -1
	if len(args) > 0 {
		if len(args) != 3 {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		if strings.ToLower(string(args[0])) != "limit" {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		if offset, err = strconv.Atoi(string(args[1])); err != nil {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
		if count, err = strconv.Atoi(string(args[2])); err != nil {
			conn.WriteError(common.ErrInvalidArgs.Error())
			return
		}
	}

	vlist, err := nd.store.ZRangeByScoreGeneric(cmd.Args[1], min, max, offset, count, reverse)
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	if needScore {
		conn.WriteArray(len(vlist) * 2)
	} else {
		conn.WriteArray(len(vlist))
	}
	for _, d := range vlist {
		conn.WriteBulk(d.Member)
		if needScore {
			conn.WriteBulkString(strconv.FormatFloat(d.Score, 'g', -1, 64))
		}
	}
}

func (nd *KVNode) zrangebyscoreCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.zrangebyscoreFunc(conn, cmd, false)
}

func (nd *KVNode) zrevrangebyscoreCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.zrangebyscoreFunc(conn, cmd, true)
}

func (nd *KVNode) zrankCommand(conn redcon.Conn, cmd redcon.Command) {
	v, err := nd.store.ZRank(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	if v < 0 {
		conn.WriteNull()
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) zrevrankCommand(conn redcon.Conn, cmd redcon.Command) {
	v, err := nd.store.ZRevRank(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError("Err: " + err.Error())
		return
	}
	if v < 0 {
		conn.WriteNull()
	} else {
		conn.WriteInt64(v)
	}
}

func (nd *KVNode) zaddCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 4 || len(cmd.Args)%2 != 0 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	_, err := getScorePairs(cmd.Args[2:])
	if err != nil {
		return nil, err
	}

	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	_, ok := v.(int64)
	if ok {
		return v, nil
	}
	return nil, errInvalidResponse
}

func (nd *KVNode) zincrbyCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 4 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	_, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return nil, err
	}

	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	rsp, ok := v.(float64)
	if ok {
		return []byte(strconv.FormatFloat(rsp, 'g', -1, 64)), nil
	}
	return nil, errInvalidResponse
}

func (nd *KVNode) zremrangebyrankCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 4 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}
	_, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	_, err = strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return nil, err
	}

	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	_, ok := v.(int64)
	if ok {
		return v, nil
	}
	return nil, errInvalidResponse
}

func (nd *KVNode) zremrangebyscoreCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 4 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}

	_, _, err := getScoreRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		return nil, err
	}
	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	_, ok := v.(int64)
	if ok {
		return v, nil
	}
	return nil, errInvalidResponse

}

func (nd *KVNode) zremrangebylexCommand(cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 4 {
		err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
		return nil, err
	}

	_, _, _, err := getLexRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		return nil, err
	}
	_, v, err := rebuildFirstKeyAndPropose(nd, cmd, nil)
	if err != nil {
		return nil, err
	}
	_, ok := v.(int64)
	if ok {
		return v, nil
	}
	return nil, errInvalidResponse
}

func getScorePairs(args [][]byte) ([]common.ScorePair, error) {
	mlist := make([]common.ScorePair, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		s, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return nil, err
		}
		mlist = append(mlist, common.ScorePair{Score: s, Member: args[i+1]})
	}

	return mlist, nil
}

func (kvsm *kvStoreSM) localZaddCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	mlist, err := getScorePairs(cmd.Args[2:])
	if err != nil {
		return nil, err
	}
	v, err := kvsm.store.ZAdd(ts, cmd.Args[1], mlist...)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (kvsm *kvStoreSM) localZincrbyCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	delta, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return nil, err
	}
	return kvsm.store.ZIncrBy(ts, cmd.Args[1], delta, cmd.Args[3])
}

func (kvsm *kvStoreSM) localZremCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len(cmd.Args) < 3 {
		return nil, common.ErrInvalidArgs
	}

	return kvsm.store.ZRem(ts, cmd.Args[1], cmd.Args[2:]...)
}

func (kvsm *kvStoreSM) localZremrangebyrankCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	stop, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		return nil, err
	}

	return kvsm.store.ZRemRangeByRank(ts, cmd.Args[1], int(start), int(stop))
}

func (kvsm *kvStoreSM) localZremrangebyscoreCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	min, max, err := getScoreRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		return nil, err
	}
	return kvsm.store.ZRemRangeByScore(ts, cmd.Args[1], min, max)
}

func (kvsm *kvStoreSM) localZremrangebylexCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	min, max, rt, err := getLexRange(cmd.Args[2], cmd.Args[3])
	if err != nil {
		return nil, err
	}
	return kvsm.store.ZRemRangeByLex(ts, cmd.Args[1], min, max, rt)
}

func (kvsm *kvStoreSM) localZclearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, common.ErrInvalidArgs
	}
	return kvsm.store.ZClear(cmd.Args[1])
}

func (kvsm *kvStoreSM) localZMClearCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	var count int64
	for _, zkey := range cmd.Args[1:] {
		if _, err := kvsm.store.ZClear(zkey); err != nil {
			return count, err
		} else {
			count++
		}
	}
	return count, nil
}

func (kvsm *kvStoreSM) localZFixKeyCommand(cmd redcon.Command, ts int64) (interface{}, error) {
	kvsm.store.ZFixKey(ts, cmd.Args[1])
	return nil, nil
}

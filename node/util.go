package node

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
)

var nodeLog = common.NewLevelLogger(common.LOG_INFO, common.NewLogger())
var syncerOnly int32
var logMaybeConflictDisabled int32
var syncerOnlyChangedTs int64

func SetLogLevel(level int) {
	nodeLog.SetLevel(int32(level))
}

func SetLogger(level int32, logger common.Logger) {
	nodeLog.SetLevel(level)
	nodeLog.Logger = logger
}

func SetSyncerOnly(enable bool) {
	if enable {
		atomic.StoreInt32(&syncerOnly, 1)
	} else {
		atomic.StoreInt32(&syncerOnly, 0)
		atomic.StoreInt64(&syncerOnlyChangedTs, time.Now().UnixNano())
	}
}

func SwitchDisableMaybeConflictLog(disable bool) {
	if disable {
		atomic.StoreInt32(&logMaybeConflictDisabled, 1)
	} else {
		atomic.StoreInt32(&logMaybeConflictDisabled, 0)
	}
}

func MaybeConflictLogDisabled() bool {
	return atomic.LoadInt32(&logMaybeConflictDisabled) == 1
}

func GetSyncedOnlyChangedTs() int64 {
	return atomic.LoadInt64(&syncerOnlyChangedTs)
}

func IsSyncerOnly() bool {
	return atomic.LoadInt32(&syncerOnly) == 1
}

func checkOKRsp(cmd redcon.Command, v interface{}) (interface{}, error) {
	return "OK", nil
}

func checkAndRewriteIntRsp(cmd redcon.Command, v interface{}) (interface{}, error) {
	if rsp, ok := v.(int64); ok {
		return rsp, nil
	}
	return nil, errInvalidResponse
}

func checkAndRewriteBulkRsp(cmd redcon.Command, v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	rsp, ok := v.([]byte)
	if ok {
		return rsp, nil
	}
	return nil, errInvalidResponse
}

func buildCommand(args [][]byte) redcon.Command {
	return common.BuildCommand(args)
}

// we can only use redis v2 for single key write command, otherwise we need cut namespace for different keys in different command
func rebuildFirstKeyAndPropose(kvn *KVNode, cmd redcon.Command, f common.CommandRspFunc) (interface{}, error) {

	var rsp *FutureRsp
	var err error
	if !UseRedisV2 {
		var key []byte
		key, err = common.CutNamesapce(cmd.Args[1])
		if err != nil {
			return nil, err
		}

		orig := cmd.Args[1]
		cmd.Args[1] = key
		ncmd := buildCommand(cmd.Args)
		rsp, err = kvn.RedisProposeAsync(ncmd.Raw)
		cmd.Args[1] = orig
	} else {
		rsp, err = kvn.RedisV2ProposeAsync(cmd.Raw)
	}
	if err != nil {
		return nil, err
	}
	if f != nil {
		rsp.rspHandle = func(r interface{}) (interface{}, error) {
			return f(cmd, r)
		}
	}
	return rsp, err
}

func wrapReadCommandK(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key, err := common.CutNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKSubkey(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key, err := common.CutNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKSubkeySubkey(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key, err := common.CutNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKAnySubkey(f common.CommandFunc) common.CommandFunc {
	return wrapReadCommandKAnySubkeyN(f, 0)
}

func wrapReadCommandKAnySubkeyN(f common.CommandFunc, minSubLen int) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2+minSubLen {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key, err := common.CutNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		cmd.Args[1] = key
		f(conn, cmd)
	}
}

func wrapReadCommandKK(f common.CommandFunc) common.CommandFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		if len(cmd.Args[1:]) > common.MAX_BATCH_NUM {
			conn.WriteError(errTooMuchBatchSize.Error())
			return
		}
		for i := 1; i < len(cmd.Args); i++ {
			key, err := common.CutNamesapce(cmd.Args[i])
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			cmd.Args[i] = key
		}
		f(conn, cmd)
	}
}

func wrapWriteCommandK(kvn *KVNode, preCheck func(key []byte) (bool, interface{}, error), f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) != 2 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		if preCheck != nil {
			key, err := common.CutNamesapce(cmd.Args[1])
			if err != nil {
				return nil, err
			}
			needContinue, rsp, err := preCheck(key)
			if err != nil {
				return nil, err
			}
			if !needContinue {
				return f(cmd, rsp)
			}
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKSubkey(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) != 3 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKSubkeySubkey(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 3 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKAnySubkey(kvn *KVNode, f common.CommandRspFunc, minSubKeyLen int) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2+minSubKeyLen {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}
func wrapWriteCommandKAnySubkeyAndMax(kvn *KVNode, f common.CommandRspFunc, minSubKeyLen int, maxSubKeyLen int) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2+minSubKeyLen {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if len(cmd.Args) > 2+maxSubKeyLen {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKV(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) != 3 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKVV(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) != 4 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapWriteCommandKSubkeyV(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return wrapWriteCommandKVV(kvn, f)
}

func wrapWriteCommandKSubkeyVSubkeyV(kvn *KVNode, f common.CommandRspFunc) common.WriteCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 4 || len(cmd.Args[2:])%2 != 0 {
			err := fmt.Errorf("ERR wrong number arguments for '%v' command", string(cmd.Args[0]))
			return nil, err
		}
		if len(cmd.Args[2:])/2 > common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		if err := common.CheckKey(cmd.Args[1]); err != nil {
			return nil, err
		}
		rsp, err := rebuildFirstKeyAndPropose(kvn, cmd, f)
		return rsp, err
	}
}

func wrapMergeCommand(f common.MergeCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		key, err := common.CutNamesapce(cmd.Args[1])
		if err != nil {
			return nil, err
		}
		cmd.Args[1] = key

		return f(cmd)
	}
}

func wrapMergeCommandKK(f common.MergeCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
		}
		if len(cmd.Args[1:]) > common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		for i := 1; i < len(cmd.Args); i++ {
			key, err := common.CutNamesapce(cmd.Args[i])
			if err != nil {
				return nil, err
			}
			cmd.Args[i] = key
		}
		return f(cmd)
	}
}

func wrapWriteMergeCommandKK(kvn *KVNode, f common.MergeWriteCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 2 {
			return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
		}
		args := cmd.Args[1:]
		if len(args) > common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		for i, v := range args {
			key, err := common.CutNamesapce(v)
			if err != nil {
				return nil, err
			}
			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)
		rsp, err := kvn.RedisPropose(ncmd.Raw)
		if err != nil {
			return nil, err
		}
		if f != nil {
			return f(cmd, rsp)
		}
		return rsp, nil
	}
}

func wrapWriteMergeCommandKVKV(kvn *KVNode, f common.MergeWriteCommandFunc) common.MergeCommandFunc {
	return func(cmd redcon.Command) (interface{}, error) {
		if len(cmd.Args) < 3 || len(cmd.Args[1:])%2 != 0 {
			return nil, fmt.Errorf("ERR wrong number arguments for '%s' command", string(cmd.Args[0]))
		}
		if len(cmd.Args[1:])/2 > common.MAX_BATCH_NUM {
			return nil, errTooMuchBatchSize
		}
		args := cmd.Args[1:]
		for i, v := range args {
			if i%2 != 0 {
				continue
			}
			key, err := common.CutNamesapce(v)
			if err != nil {
				return nil, err
			}
			args[i] = key
		}
		ncmd := buildCommand(cmd.Args)

		rsp, err := kvn.RedisPropose(ncmd.Raw)
		if err != nil {
			return nil, err
		}
		if f != nil {
			return f(cmd, rsp)
		}
		return rsp, nil
	}
}

type notifier struct {
	c   chan struct{}
	err error
}

func newNotifier() *notifier {
	return &notifier{
		c: make(chan struct{}),
	}
}

func (nc *notifier) notify(err error) {
	nc.err = err
	close(nc.c)
}

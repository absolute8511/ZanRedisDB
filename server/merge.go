package server

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/absolute8511/redcon"
)

func dispatchHandlersAndWait(cmdName string, handlers []common.MergeCommandFunc, cmds []redcon.Command, concurrent bool) []interface{} {
	mergeStart := time.Now()
	defer func(start time.Time) {
		cost := time.Since(mergeStart)
		if cost >= time.Second {
			sLog.Infof("slow merge command: %v, cost: %v", cmdName, cost)
		}
	}(mergeStart)

	var err error
	var wg sync.WaitGroup
	var results []interface{}
	length := len(handlers)
	results = make([]interface{}, length)
	for i, h := range handlers {
		if !concurrent {
			results[i], err = h(cmds[i])
			if err != nil {
				sLog.Infof("part of merge command error:%v, %v", string(cmds[i].Raw), err.Error())
				results[i] = err
			}
		} else {
			wg.Add(1)
			go func(index int, handle common.MergeCommandFunc) {
				defer wg.Done()
				var err error
				results[index], err = handle(cmds[index])
				if err != nil {
					sLog.Infof("part of merge command error:%v, %v", string(cmds[i].Raw), err.Error())
					results[index] = err
				}
			}(i, h)
		}
	}
	wg.Wait()
	return results
}

func (s *Server) doMergeCommand(conn redcon.Conn, cmd redcon.Command) {
	cmdName := qcmdlower(cmd.Args[0])

	if common.IsMergeScanCommand(cmdName) {
		if common.IsFullScanCommand(cmdName) {
			s.doMergeFullScan(conn, cmd)
		} else {
			s.doMergeScan(conn, cmd)
		}
	} else if common.IsMergeIndexSearchCommand(cmdName) {
		s.doMergeIndexSearch(conn, cmd)
	} else if common.IsMergeKeysCommand(cmdName) {
		// current we only handle the command which keys may across multi partitions and the
		// response is all the same. So if the response order is need for keys, we can not handle
		// it as simple merged keys. Maybe need to do in sdk.
		s.doMergeKeysCommand(conn, cmdName, cmd)
	} else {
		conn.WriteError("not supported merge command " + string(cmdName))
	}
}

func (s *Server) getHandlersForKeys(cmdName string,
	origArgs [][]byte) ([]common.MergeCommandFunc, []redcon.Command, bool, error) {
	cmdArgMap := make(map[string][][]byte)
	handlerMap := make(map[string]common.MergeCommandFunc)
	var namespace string
	hasWrite := false
	hasRead := false
	origKeys := origArgs
	var vals [][]byte
	if cmdName == "plset" {
		// for command which args is [key val key val]
		if sLog.Level() >= common.LOG_DETAIL {
			sLog.Debugf("merge plset command %v", origArgs)
		}
		origKeys = make([][]byte, 0, len(origArgs)/2)
		vals = make([][]byte, 0, len(origArgs)/2)
		for i := 0; i < len(origArgs)-1; i = i + 2 {
			origKeys = append(origKeys, origArgs[i])
			vals = append(vals, origArgs[i+1])
		}
	}
	for kindex, arg := range origKeys {
		ns, realKey, err := common.ExtractNamesapce(arg)
		if err != nil {
			sLog.Infof("failed to get the namespace for key:%v", string(arg))
			return nil, nil, hasWrite, err
		}
		if namespace != "" && ns != namespace {
			return nil, nil, hasWrite, common.ErrInvalidArgs
		}
		if namespace == "" {
			namespace = ns
		}
		nsNode, err := s.nsMgr.GetNamespaceNodeWithPrimaryKey(ns, realKey)
		if err != nil {
			sLog.Infof("failed to get the namespace %s node for pk key:%v, rawkey: %v", ns, string(realKey), string(arg))
			return nil, nil, hasWrite, err
		}
		f, isWrite, ok := nsNode.Node.GetMergeHandler(cmdName)
		if !ok {
			return nil, nil, hasWrite, errInvalidCommand
		}
		if isWrite {
			hasWrite = true
		} else {
			hasRead = true
		}
		if hasWrite && hasRead {
			// never happen
			return nil, nil, false, errInvalidCommand
		}
		if !isWrite && !nsNode.Node.IsLead() && (atomic.LoadInt32(&allowStaleRead) == 0) {
			// read only to leader to avoid stale read
			// TODO: also read command can request the raft read index if not leader
			return nil, nil, hasWrite, node.ErrNamespaceNoLeader
		}
		handlerMap[nsNode.FullName()] = f
		cmdArgs, ok := cmdArgMap[nsNode.FullName()]
		if !ok {
			cmdArgs = make([][]byte, 0, len(origKeys))
			cmdArgs = append(cmdArgs, []byte(cmdName))
		}
		cmdArgs = append(cmdArgs, arg)
		if cmdName == "plset" {
			cmdArgs = append(cmdArgs, vals[kindex])
		}
		cmdArgMap[nsNode.FullName()] = cmdArgs
	}

	handlers := make([]common.MergeCommandFunc, 0, len(handlerMap))
	cmds := make([]redcon.Command, 0, len(handlerMap))
	for name, handler := range handlerMap {
		handlers = append(handlers, handler)
		cmds = append(cmds, buildCommand(cmdArgMap[name]))
	}
	return handlers, cmds, hasWrite, nil
}

func (s *Server) doMergeKeysCommand(conn redcon.Conn, cmdName string, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		err := fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
		conn.WriteError(err.Error())
		return
	}

	cmds, results, err := s.dispatchAndWaitMergeCmd(cmd)
	if err != nil {
		sLog.Infof("merge command %v error:%v", string(cmd.Raw), err.Error())
		conn.WriteError(err.Error())
		return
	}
	if sLog.Level() >= common.LOG_DETAIL {
		sLog.Debugf("merge command return %v", results)
	}
	switch cmdName {
	case "exists", "del":
		cnt := int64(0)
		for _, ret := range results {
			if v, ok := ret.(int64); ok {
				cnt += v
			}
		}
		conn.WriteInt64(cnt)
		return
	case "plset":
		for i, ret := range results {
			if err, ok := ret.(error); ok {
				for ci := 1; ci < len(cmds[i].Args); ci += 2 {
					conn.WriteError("ERR :" + err.Error())
				}
			} else {
				for ci := 1; ci < len(cmds[i].Args); ci += 2 {
					conn.WriteString("OK")
				}
			}
		}
		return
	default:
		sLog.Infof("merge command error:%v", cmdName)
		conn.WriteError(errInvalidCommand.Error())
		return
	}
}

func (s *Server) dispatchAndWaitMergeCmd(cmd redcon.Command) ([]redcon.Command, []interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
	}
	handlers, cmds, concurrent, err := s.GetMergeHandlers(cmd)
	if err != nil {
		return nil, nil, err
	}
	return cmds, dispatchHandlersAndWait(string(cmd.Args[0]), handlers, cmds, concurrent), nil
}

func (s *Server) GetMergeHandlers(cmd redcon.Command) ([]common.MergeCommandFunc, []redcon.Command, bool, error) {
	if len(cmd.Args) < 2 {
		return nil, nil, false, fmt.Errorf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
	}
	rawKey := cmd.Args[1]

	namespace, realKey, err := common.ExtractNamesapce(rawKey)
	if err != nil {
		sLog.Infof("failed to get the namespace of the redis command:%v", string(rawKey))
		return nil, nil, false, err
	}

	nodes, err := s.nsMgr.GetNamespaceNodes(namespace, true)
	if err != nil {
		return nil, nil, false, err
	}

	cmdName := strings.ToLower(string(cmd.Args[0]))
	var cmds map[string]redcon.Command
	//do nodes filter
	if common.IsMergeScanCommand(cmdName) {
		cmds, err = s.doScanNodesFilter(realKey, namespace, cmd, nodes)
		if err != nil {
			return nil, nil, false, err
		}
	} else if common.IsMergeKeysCommand(cmdName) {
		return s.getHandlersForKeys(cmdName, cmd.Args[1:])
	} else {
		cmds = make(map[string]redcon.Command)
		for k := range nodes {
			newCmd := common.DeepCopyCmd(cmd)
			cmds[k] = newCmd
		}
	}

	var handlers []common.MergeCommandFunc
	var commands []redcon.Command
	needConcurrent := true
	for k, v := range nodes {
		newCmd := cmds[k]
		h, isWrite, ok := v.Node.GetMergeHandler(cmdName)
		if ok {
			if !isWrite && !v.Node.IsLead() && (atomic.LoadInt32(&allowStaleRead) == 0) {
				// read only to leader to avoid stale read
				// TODO: also read command can request the raft read index if not leader
				return nil, nil, needConcurrent, node.ErrNamespaceNotLeader
			}
			handlers = append(handlers, h)
			commands = append(commands, newCmd)
		}
	}

	if len(handlers) <= 0 {
		return nil, nil, needConcurrent, common.ErrInvalidCommand
	}

	return handlers, commands, needConcurrent, nil
}

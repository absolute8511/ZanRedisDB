package server

import (
	"sync"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
)

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
	} else {
		conn.WriteError("not supported merge command " + string(cmdName))
	}
}

func (s *Server) dispatchAndWaitMergeCmd(cmd redcon.Command) ([]interface{}, error) {
	mergeStart := time.Now()
	defer func(start time.Time) {
		cost := time.Since(mergeStart)
		if cost >= time.Second {
			sLog.Infof("slow merge command: %v, cost: %v", string(cmd.Raw), cost)
		}
	}(mergeStart)

	if len(cmd.Args) < 2 {
		return nil, common.ErrInvalidArgs
	}
	var err error
	var wg sync.WaitGroup
	var results []interface{}
	handlers, cmds, err := s.GetMergeHandlers(cmd)
	if err != nil {
		return nil, err
	}
	length := len(handlers)
	results = make([]interface{}, length)
	for i, h := range handlers {
		wg.Add(1)
		go func(index int, handle common.MergeCommandFunc) {
			defer wg.Done()
			var err error
			results[index], err = handle(cmds[index])
			if err != nil {
				results[index] = err
			}
		}(i, h)
	}
	wg.Wait()
	return results, nil
}

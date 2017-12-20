package server

import (
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/redcon"
)

var (
	errInvalidCommand = errors.New("invalid command")
	costStatsLevel    int32
)

func (s *Server) serverRedis(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			sLog.Infof("handle redis command %v panic: %s:%v", string(cmd.Args[0]), buf, e)
			conn.Close()
		}
	}()

	_, cmd, err := pipelineCommand(conn, cmd)
	if err != nil {
		conn.WriteError("pipeline error '" + err.Error() + "'")
		return
	}
	cmdName := qcmdlower(cmd.Args[0])
	switch cmdName {
	case "detach":
		hconn := conn.Detach()
		sLog.Infof("connection has been detached")
		go func() {
			defer hconn.Close()
			hconn.WriteString("OK")
			hconn.Flush()
		}()
	case "ping":
		conn.WriteString("PONG")
	case "auth":
		// TODO: add auth here
		conn.WriteString("OK")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "info":
		s := s.GetStats(false)
		d, _ := json.MarshalIndent(s, "", " ")
		conn.WriteBulkString(string(d))
	default:
		if common.IsMergeCommand(cmdName) {
			s.doMergeCommand(conn, cmd)
		} else {
			var start time.Time
			level := atomic.LoadInt32(&costStatsLevel)
			if level > 0 {
				start = time.Now()
			}
			h, cmd, err := s.GetHandler(cmdName, cmd)
			cmdStr := string(cmd.Args[0])
			if len(cmd.Args) > 1 {
				cmdStr += ", " + string(cmd.Args[1])
				if level > 4 && len(cmd.Args) > 2 {
					for _, arg := range cmd.Args[2:] {
						cmdStr += "," + string(arg)
					}
				}
			}
			if err == nil {
				h(conn, cmd)
			} else {
				conn.WriteError(err.Error() + " : ERR handle command " + cmdStr)
			}
			if level > 0 {
				cost := time.Since(start)
				if cost >= time.Second ||
					(level > 1 && cost > time.Millisecond*500) ||
					(level > 2 && cost > time.Millisecond*100) ||
					(level > 3 && cost > time.Millisecond) ||
					(level > 4) {
					sLog.Infof("slow command %v cost %v", cmdStr, cost)
				}
			}
		}
	}
}

func (s *Server) serveRedisAPI(port int, stopC <-chan struct{}) {
	redisS := redcon.NewServer(
		":"+strconv.Itoa(port),
		s.serverRedis,
		func(conn redcon.Conn) bool {
			//sLog.Infof("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			if err != nil {
				sLog.Infof("closed: %s, err: %v", conn.RemoteAddr(), err)
			}
		},
	)
	go func() {
		err := redisS.ListenAndServe()
		if err != nil {
			sLog.Fatalf("failed to start the redis server: %v", err)
		}
	}()
	<-stopC
	redisS.Close()
	sLog.Infof("redis api server exit\n")
}

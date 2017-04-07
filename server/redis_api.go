package server

import (
	"encoding/json"
	"errors"
	"github.com/tidwall/redcon"
	"runtime"
	"strconv"
)

var (
	errInvalidCommand = errors.New("invalid command")
)

func (self *Server) serverRedis(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			sLog.Infof("handle redis command panic: %s:%v", buf, e)
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
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "info":
		s := self.GetStats(false)
		d, _ := json.MarshalIndent(s, "", " ")
		conn.WriteBulkString(string(d))
	default:
		h, cmd, err := self.GetHandler(cmdName, cmd)
		if err == nil {
			h(conn, cmd)
		} else {
			conn.WriteError(err.Error() + " : ERR handle command " + string(cmd.Args[0]))
		}
	}
}

func (self *Server) serveRedisAPI(port int, stopC <-chan struct{}) {
	redisS := redcon.NewServer(
		":"+strconv.Itoa(port),
		self.serverRedis,
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

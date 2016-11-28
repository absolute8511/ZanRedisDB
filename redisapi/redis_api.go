package redisapi

import (
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
	"log"
	"strconv"
)

var (
	errInvalidCommand  = errors.New("invalid command")
	errSyntaxError     = errors.New("syntax error")
	errInvalidResponse = errors.New("invalid response")
)

func serverRedis(conn redcon.Conn, cmd redcon.Command) {
	_, cmd, err := pipelineCommand(conn, cmd)
	if err != nil {
		conn.WriteError("pipeline error '" + err.Error() + "'")
		return
	}
	cmdName := qcmdlower(cmd.Args[0])
	switch cmdName {
	case "detach":
		hconn := conn.Detach()
		log.Printf("connection has been detached")
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
	default:
		h, ok := common.GetHandler(cmdName)
		if ok {
			h(conn, cmd)
		} else {
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		}
	}
}

func ServeRedisAPI(port int, errorC <-chan error) {
	redisS := redcon.NewServer(
		":"+strconv.Itoa(port),
		serverRedis,
		func(conn redcon.Conn) bool {
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	go func() {
		err := redisS.ListenAndServe()
		if err != nil {
			log.Fatalf("failed to start the redis server: %v", err)
		}
	}()
	<-errorC
	redisS.Close()
	log.Printf("redis api server exit\n")
}

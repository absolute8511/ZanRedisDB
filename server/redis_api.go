package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/redcon"
)

var (
	errInvalidCommand = errors.New("invalid command")
	errMaxScanJob     = errors.New("too much scan job")
)

var (
	scanJobCount = 0
)

var (
	DIST_SCAN_KEY_SEP = []byte("@|@")
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
		if isScanCommand(cmdName) {
			if scanJobCount >= self.maxScanJob {
				conn.WriteError(errMaxScanJob.Error() + " : Err handle command " + string(cmd.Args[0]))
				return
			}
			scanJobCount++
			defer func() {
				scanJobCount--
			}()
			scanStart := time.Now()
			self.dealScanCommand(conn, cmd)
			scanCost := time.Since(scanStart)
			if scanCost >= time.Second {
				sLog.Infof("slow write command: %v, cost: %v", string(cmd.Raw), scanCost)
			}
			self.scanStats.UpdateScanStats(scanCost.Nanoseconds() / 1000)
		} else {
			h, cmd, err := self.GetHandler(cmdName, cmd)
			if err == nil {
				h(conn, cmd)
			} else {
				conn.WriteError(err.Error() + " : ERR handle command " + string(cmd.Args[0]))
			}
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

func (self *Server) dealScanCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) >= 2 {
		var err error
		var count int
		var countIndex int
		for i := 0; i < len(cmd.Args); i++ {
			if strings.ToLower(string(cmd.Args[i])) == "count" {
				if i+1 >= len(cmd.Args) {
					conn.WriteError(common.ErrInvalidArgs.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
				countIndex = i + 1

				count, err = strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
				break
			}
		}

		_, _, err = common.ExtractNamesapce(cmd.Args[1])

		if err == nil {
			result := make([]interface{}, 0)
			var chs []chan common.ScanResult

			nextCursor := []byte("")
			//			fmt.Println(string(cmd.Args[0]), string(cmd.Args[1]))
			//			ukeyIndex := bytes.IndexByte(realKey, ':')
			//			fmt.Println(ukeyIndex, ":", len(realKey))
			cmdName := qcmdlower(cmd.Args[0])
			//			if bytes.IndexByte(cmd.Args[1], '@') <= 0 && (ukeyIndex == -1 || ukeyIndex == len(realKey)-1) {
			if bytes.IndexAny(cmd.Args[1], string(DIST_SCAN_KEY_SEP)) <= 0 {
				hs, _, err := self.GetScanHandlers(cmdName, cmd)
				if err == nil {

					length := len(hs)
					everyCount := count / length
					chs = make([]chan common.ScanResult, length)

					for i, h := range hs {
						newCmd := common.DeepCopyCmd(cmd)
						chs[i] = make(chan common.ScanResult)
						newCmd.Args[countIndex] = []byte(strconv.Itoa(everyCount))
						go func(index int, cmd redcon.Command, handle common.ScanCommandFunc) {
							handle(chs[index], cmd)
						}(i, newCmd, h)
					}
				} else {
					conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
					return
				}
			} else {
				cmd.Args[1] = bytes.TrimRight(cmd.Args[1], string(DIST_SCAN_KEY_SEP))
				cursors := bytes.Split(cmd.Args[1], DIST_SCAN_KEY_SEP)

				length := len(cursors)
				everyCount := count / length
				chs = make([]chan common.ScanResult, length)

				for i, cur := range cursors {
					newCmd := common.DeepCopyCmd(cmd)
					newCmd.Args[1] = cur
					newCmd.Args[countIndex] = []byte(strconv.Itoa(everyCount))
					h, newCmd, err := self.GetScanHandler(cmdName, newCmd)
					if err != nil {
						conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
						return
					} else {
						chs[i] = make(chan common.ScanResult)
						go func(index int, cmd redcon.Command, handle common.ScanCommandFunc) {
							cmd.Args[countIndex] = []byte(strconv.Itoa(everyCount))
							handle(chs[index], cmd)
						}(i, newCmd, h)
					}
				}
			}

			for _, ch := range chs {
				res := <-ch
				if res.Error == nil {
					v := reflect.ValueOf(res.List)
					if v.Kind() != reflect.Slice {
						continue
					}

					if len(res.NextCursor) > 0 {
						//						nextCursor = append(nextCursor, []byte(ns+":")...)
						nextCursor = append(nextCursor, res.NextCursor...)
						nextCursor = append(nextCursor, DIST_SCAN_KEY_SEP...)
					}
					cnt := v.Len()
					for i := 0; i < cnt; i++ {
						result = append(result, v.Index(i).Interface())
					}

				} else {
					fmt.Println("Resp error:" + res.Error.Error())
				}
			}
			fmt.Println("#### cursor:", string(nextCursor))
			conn.WriteArray(2)
			conn.WriteBulk(nextCursor)

			switch strings.ToLower(cmdName) {
			case "hscan":
				conn.WriteArray(len(result) * 2)
				for _, v := range result {
					conn.WriteBulk(v.(common.KVRecord).Key)
					conn.WriteBulk(v.(common.KVRecord).Value)
				}
			case "zscan":
				conn.WriteArray(len(result) * 2)
				for _, v := range result {
					conn.WriteBulk(v.(common.ScorePair).Member)
					conn.WriteBulk([]byte(strconv.FormatInt(v.(common.ScorePair).Score, 10)))
				}
			default:
				conn.WriteArray(len(result))
				for _, v := range result {
					conn.WriteBulk(v.([]byte))
				}
			}

		} else {
			conn.WriteError(err.Error() + " : Err handle command " + string(cmd.Args[0]))
		}
	} else {
		conn.WriteError(common.ErrInvalidArgs.Error() + " : Err handle command " + string(cmd.Args[0]))
	}
}

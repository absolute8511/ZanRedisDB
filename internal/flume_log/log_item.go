package flume_log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"
)

var (
	hostName   string
	internalIp string
	platform   string
	pid        int
)

func init() {
	hostName, _ = os.Hostname()
	ips, _ := net.LookupHost(hostName)

	for _, ip := range ips {
		if ip != "127.0.0.1" {
			internalIp = ip
			break
		}
	}
	platform = runtime.Version()
	pid = os.Getpid()
}

func NewLogItem(level string, appName string, logIndex string) *LogItem {
	return &LogItem{
		LogHeader{
			level,
			appName,
			logIndex,
		},
		LogBody{
			App:      appName,
			Level:    level,
			Module:   logIndex,
			Platform: platform,
		},
	}
}

type LogItem struct {
	Header LogHeader
	Body   LogBody
}

func (self *LogItem) SetDetail(detail map[string]interface{}) {
	self.Body.Detail = detail
}

func (self *LogItem) SetTag(tag string) {
	self.Body.Tag = tag
}

func (self *LogItem) Bytes() []byte {
	var tmpBuf bytes.Buffer
	tmpBuf.WriteString(self.Header.String())
	tmpBuf.WriteString(" ")
	tmpBuf.Write(self.Body.Bytes())
	tmpBuf.WriteString("\n")
	return tmpBuf.Bytes()
}

type LogHeader struct {
	Level    string
	AppName  string
	LogIndex string
}

func (header *LogHeader) String() string {
	return fmt.Sprintf(
		"<158>%s %s/%s %s[%d]: topic=log.%s.%s",
		time.Now().Format("2006-01-02 15:04:05"), hostName, internalIp, header.Level, pid,
		header.AppName, header.LogIndex,
	)
}

type LogBody struct {
	App      string                 `json:"app"`
	Level    string                 `json:"level"`
	Module   string                 `json:"module"`
	Tag      string                 `json:"tag"`
	Detail   map[string]interface{} `json:"detail,omitempty"`
	Platform string                 `json:"platform"`
}

func (body *LogBody) Bytes() []byte {
	jsonb, _ := json.Marshal(body)
	return jsonb
}

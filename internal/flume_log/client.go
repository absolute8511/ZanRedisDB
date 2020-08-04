package flume_log

import (
	"errors"
)

const (
	typeInfo  = "info"
	typeWarn  = "warn"
	typeDebug = "debug"
	typeError = "error"
)

// RemoteLogClient describes the remote log client
type RemoteLogClient struct {
	AppName       string
	LogIndex      string
	ServerAddress string
	client        *FlumeClient
}

// NewClient returns a RemoteLogClient
func NewClient(address string, appName string, logIndex string) *RemoteLogClient {
	c := NewFlumeClient(address)
	return &RemoteLogClient{
		AppName:       appName,
		LogIndex:      logIndex,
		ServerAddress: address,
		client:        c,
	}
}

// Info uses info-level
func (rlc *RemoteLogClient) Info(msg string, extra interface{}) error {
	return rlc.log(typeInfo, msg, extra)
}

// Warn uses warn-level
func (rlc *RemoteLogClient) Warn(msg string, extra interface{}) error {
	return rlc.log(typeWarn, msg, extra)
}

// Debug uses debug-level
func (rlc *RemoteLogClient) Debug(msg string, extra interface{}) error {
	return rlc.log(typeDebug, msg, extra)
}

// Error uses error-level
func (rlc *RemoteLogClient) Error(msg string, extra interface{}) error {
	return rlc.log(typeError, msg, extra)
}

func (rlc *RemoteLogClient) log(level string, msg string, extra interface{}) error {
	logindex := rlc.LogIndex
	logItem := NewLogItem(level, rlc.AppName, logindex)
	logItem.SetTag(msg)

	if extra != nil {
		detail := make(map[string]interface{})
		detail["extra"] = []interface{}{extra}
		logItem.SetDetail(detail)
	}

	if rlc.client != nil {
		return rlc.client.SendLog(logItem.Bytes())
	}
	return errors.New("no client")
}

// Stop stops all conns
func (rlc *RemoteLogClient) Stop() {
	if rlc.client != nil {
		rlc.client.Stop()
	}
}

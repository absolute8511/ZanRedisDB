package common

import (
	"fmt"
	"github.com/absolute8511/glog"
	"log"
	"os"
	"sync/atomic"
)

type Logger interface {
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}

type defaultLogger struct {
	logger *log.Logger
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}

func NewDefaultLogger(module string) *defaultLogger {
	return &defaultLogger{
		logger: log.New(os.Stderr, module, log.LstdFlags|log.Lmicroseconds|log.Lshortfile),
	}
}

func (self *defaultLogger) Output(maxdepth int, s string) error {
	self.logger.Output(maxdepth+1, s)
	return nil
}

func (self *defaultLogger) OutputErr(maxdepth int, s string) error {
	self.logger.Output(maxdepth+1, header("ERR", s))
	return nil
}

func (self *defaultLogger) OutputWarning(maxdepth int, s string) error {
	self.logger.Output(maxdepth+1, header("WARN", s))
	return nil
}

type GLogger struct {
}

func (self *GLogger) Output(maxdepth int, s string) error {
	glog.InfoDepth(maxdepth, s)
	return nil
}

func (self *GLogger) OutputErr(maxdepth int, s string) error {
	glog.ErrorDepth(maxdepth, s)
	return nil
}

func (self *GLogger) OutputWarning(maxdepth int, s string) error {
	glog.WarningDepth(maxdepth, s)
	return nil
}

const (
	LOG_ERR int32 = iota
	LOG_WARN
	LOG_INFO
	LOG_DEBUG
	LOG_DETAIL
)

type LevelLogger struct {
	Logger Logger
	level  int32
}

func NewLevelLogger(level int32, l Logger) *LevelLogger {
	return &LevelLogger{
		Logger: l,
		level:  level,
	}
}

func (self *LevelLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *LevelLogger) Level() int32 {
	return atomic.LoadInt32(&self.level)
}

func (self *LevelLogger) InfoDepth(d int, l string) {
	if self.Logger != nil && self.Level() >= LOG_INFO {
		self.Logger.Output(2+d, l)
	}
}

func (self *LevelLogger) Infof(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_INFO {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Debugf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_DEBUG {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Errorf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Warningf(f string, args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_WARN {
		self.Logger.OutputWarning(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Fatalf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
	os.Exit(1)
}

func (self *LevelLogger) Panicf(f string, args ...interface{}) {
	s := fmt.Sprintf(f, args...)
	if self.Logger != nil {
		self.Logger.OutputErr(2, s)
	}
	panic(s)
}

func (self *LevelLogger) Info(args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_INFO {
		self.Logger.Output(2, fmt.Sprint(args...))
	}
}

func (self *LevelLogger) Debug(args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_DEBUG {
		self.Logger.Output(2, fmt.Sprint(args...))
	}
}

func (self *LevelLogger) Error(args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprint(args...))
	}
}

func (self *LevelLogger) Warning(args ...interface{}) {
	if self.Logger != nil && self.Level() >= LOG_WARN {
		self.Logger.OutputWarning(2, fmt.Sprint(args...))
	}
}

func (self *LevelLogger) Fatal(args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprint(args...))
	}
	os.Exit(1)
}

func (self *LevelLogger) Panic(args ...interface{}) {
	s := fmt.Sprint(args...)
	if self.Logger != nil {
		self.Logger.OutputErr(2, s)
	}
	panic(s)
}

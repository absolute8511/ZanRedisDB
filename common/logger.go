package common

import (
	"fmt"
	"github.com/absolute8511/glog"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
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
		logger: log.New(os.Stdout, module, log.LstdFlags|log.Lmicroseconds|log.Lshortfile),
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

func (self *LevelLogger) DebugDepth(d int, l string) {
	if self.Logger != nil && self.Level() >= LOG_DEBUG {
		self.Logger.Output(2+d, l)
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

func (self *LevelLogger) ErrorDepth(d int, l string) {
	if self.Logger != nil {
		self.Logger.OutputErr(2+d, l)
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

var (
	defaultMergePeriod     = time.Second
	defaultTimeOutputScale = 10 * time.Millisecond
	outputInterval         = time.Second
)

// line represents a log line that can be printed out
// through Logger.
type line struct {
	level int32
	str   string
}

func (l line) append(s string) line {
	return line{
		level: l.level,
		str:   l.str + " " + s,
	}
}

// status represents the merge status of a line.
type status struct {
	period time.Duration
	start  time.Time // start time of latest merge period
	count  int       // number of merged lines from starting
}

func (s *status) isInMergePeriod(now time.Time) bool {
	return s.period == 0 || s.start.Add(s.period).After(now)
}

func (s *status) isEmpty() bool { return s.count == 0 }

func (s *status) summary(now time.Time) string {
	ts := s.start.Round(defaultTimeOutputScale)
	took := now.Round(defaultTimeOutputScale).Sub(ts)
	return fmt.Sprintf("[merged %d repeated lines in %s]", s.count, took)
}

func (s *status) reset(now time.Time) {
	s.start = now
	s.count = 0
}

// MergeLogger supports merge logging, which merges repeated log lines
// and prints summary log lines instead.
//
// For merge logging, MergeLogger prints out the line when the line appears
// at the first time. MergeLogger holds the same log line printed within
// defaultMergePeriod, and prints out summary log line at the end of defaultMergePeriod.
// It stops merging when the line doesn't appear within the
// defaultMergePeriod.
type MergeLogger struct {
	*LevelLogger

	mu      sync.Mutex // protect statusm
	statusm map[line]*status
}

func NewMergeLogger(logger *LevelLogger) *MergeLogger {
	l := &MergeLogger{
		LevelLogger: logger,
		statusm:     make(map[line]*status),
	}
	go l.outputLoop()
	return l
}

func (l *MergeLogger) MergeInfo(entries ...interface{}) {
	l.merge(line{
		level: LOG_INFO,
		str:   fmt.Sprint(entries...),
	})
}

func (l *MergeLogger) MergeInfof(format string, args ...interface{}) {
	l.merge(line{
		level: LOG_INFO,
		str:   fmt.Sprintf(format, args...),
	})
}

func (l *MergeLogger) MergeWarning(entries ...interface{}) {
	l.merge(line{
		level: LOG_WARN,
		str:   fmt.Sprint(entries...),
	})
}

func (l *MergeLogger) MergeWarningf(format string, args ...interface{}) {
	l.merge(line{
		level: LOG_WARN,
		str:   fmt.Sprintf(format, args...),
	})
}

func (l *MergeLogger) MergeError(entries ...interface{}) {
	l.merge(line{
		level: LOG_ERR,
		str:   fmt.Sprint(entries...),
	})
}

func (l *MergeLogger) MergeErrorf(format string, args ...interface{}) {
	l.merge(line{
		level: LOG_ERR,
		str:   fmt.Sprintf(format, args...),
	})
}

func (l *MergeLogger) merge(ln line) {
	l.mu.Lock()

	// increase count if the logger is merging the line
	if status, ok := l.statusm[ln]; ok {
		status.count++
		l.mu.Unlock()
		return
	}

	// initialize status of the line
	l.statusm[ln] = &status{
		period: defaultMergePeriod,
		start:  time.Now(),
	}
	// release the lock before IO operation
	l.mu.Unlock()
	// print out the line at its first time
	if ln.level >= l.Level() {
		if ln.level >= LOG_INFO {
			l.LevelLogger.Info(ln.str)
		} else if ln.level == LOG_WARN {
			l.LevelLogger.Warning(ln.str)
		} else if ln.level <= LOG_ERR {
			l.LevelLogger.Error(ln.str)
		}
	}
}

func (l *MergeLogger) outputLoop() {
	for now := range time.Tick(outputInterval) {
		var outputs []line

		l.mu.Lock()
		for ln, status := range l.statusm {
			if status.isInMergePeriod(now) {
				continue
			}
			if status.isEmpty() {
				delete(l.statusm, ln)
				continue
			}
			outputs = append(outputs, ln.append(status.summary(now)))
			status.reset(now)
		}
		l.mu.Unlock()

		for _, o := range outputs {
			if o.level >= l.Level() {
				if o.level >= LOG_INFO {
					l.LevelLogger.Info(o.str)
				} else if o.level == LOG_WARN {
					l.LevelLogger.Warning(o.str)
				} else if o.level <= LOG_ERR {
					l.LevelLogger.Error(o.str)
				}
			}
		}
	}
}

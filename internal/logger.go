package internal

import (
	"log"
	"sync/atomic"
)

type StdLogger struct {
	Level Level
}

func (s StdLogger) Error(args ...interface{}) {
	s.log(ErrorLevel, args...)
}

func (s StdLogger) Info(args ...interface{}) {
	s.log(InfoLevel, args...)
}

func (s StdLogger) Debug(args ...interface{}) {
	s.log(DebugLevel, args...)
}
func (s StdLogger) Trace(args ...interface{}) {
	s.log(TraceLevel, args...)
}

func (s StdLogger) Errorf(format string, args ...interface{}) {
	s.logf(ErrorLevel, format, args...)
}
func (s StdLogger) Infof(format string, args ...interface{}) {
	s.logf(InfoLevel, format, args...)
}

func (s StdLogger) Debugf(format string, args ...interface{}) {
	s.logf(DebugLevel, format, args...)
}
func (s StdLogger) Tracef(format string, args ...interface{}) {
	s.logf(TraceLevel, format, args...)
}

func (s StdLogger) logf(level Level, format string, args ...interface{}) {
	if s.isLevelEnabled(level) {
		log.Printf(format, args...)
	}
}

func (s StdLogger) log(level Level, args ...interface{}) {
	if s.isLevelEnabled(level) {
		log.Println(args...)
	}
}

func (s *StdLogger) isLevelEnabled(level Level) bool {
	return s.level() >= level
}

func (s *StdLogger) level() Level {
	return Level(atomic.LoadUint32((*uint32)(&s.Level)))
}

type Level uint32

const (
	ErrorLevel Level = iota
	InfoLevel
	DebugLevel
	TraceLevel
)

package goamqp

import (
	"log"
	"sync/atomic"
)

// NewStdLogger returns a new StdLogger at Info level
func NewStdLogger() StdLogger {
	return StdLogger{Level: infoLevel}
}

// StdLogger implements the Logger interface
type StdLogger struct {
	Level level
}

// Error log
func (s StdLogger) Error(args ...interface{}) {
	s.log(errorLevel, args...)
}

// Info log
func (s StdLogger) Info(args ...interface{}) {
	s.log(infoLevel, args...)
}

// Debug log
func (s StdLogger) Debug(args ...interface{}) {
	s.log(debugLevel, args...)
}

// Trace log
func (s StdLogger) Trace(args ...interface{}) {
	s.log(traceLevel, args...)
}

// Errorf log
func (s StdLogger) Errorf(format string, args ...interface{}) {
	s.logf(errorLevel, format, args...)
}

// Infof log
func (s StdLogger) Infof(format string, args ...interface{}) {
	s.logf(infoLevel, format, args...)
}

// Debugf log
func (s StdLogger) Debugf(format string, args ...interface{}) {
	s.logf(debugLevel, format, args...)
}

// Tracef log
func (s StdLogger) Tracef(format string, args ...interface{}) {
	s.logf(traceLevel, format, args...)
}

func (s StdLogger) logf(level level, format string, args ...interface{}) {
	if s.isLevelEnabled(level) {
		log.Printf(format, args...)
	}
}

func (s StdLogger) log(level level, args ...interface{}) {
	if s.isLevelEnabled(level) {
		log.Println(args...)
	}
}

func (s *StdLogger) isLevelEnabled(level level) bool {
	return s.level() >= level
}

func (s *StdLogger) level() level {
	return level(atomic.LoadUint32((*uint32)(&s.Level)))
}

type level uint32

const (
	errorLevel level = iota
	infoLevel
	debugLevel
	traceLevel
)

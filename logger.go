package goamqp

import (
	"gitlab.com/sparetimecoders/goamqp/internal"
)

type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Trace(args ...interface{})
	Tracef(format string, args ...interface{})
}

func StdLogger() Logger {
	return internal.StdLogger{Level: internal.InfoLevel}
}

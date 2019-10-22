package goamqp

// Logger is the interface to use in applications
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

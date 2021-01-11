package goamqp

import (
	"fmt"
)

// Logger represents the logging API
// Maps to Apex log interface for convenience
// https://github.com/apex/log/blob/master/interface.go
type Logger interface {
	Debug(string)
	Info(string)
	Warn(string)
	Error(string)
	Fatal(string)
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
}

type noOpLogger struct{}

func (m *noOpLogger) Debug(s string) {
}

func (m *noOpLogger) Info(s string) {
}

func (m *noOpLogger) Warn(s string) {
}

func (m *noOpLogger) Error(s string) {
}

func (m *noOpLogger) Fatal(s string) {
}

func (m *noOpLogger) Debugf(s string, i ...interface{}) {
}

func (m *noOpLogger) Infof(s string, i ...interface{}) {
}

func (m *noOpLogger) Warnf(s string, i ...interface{}) {
}

func (m *noOpLogger) Errorf(s string, i ...interface{}) {
}

func (m *noOpLogger) Fatalf(s string, i ...interface{}) {
}

var _ Logger = &noOpLogger{}

type stdOutLogger struct {
}

func (l stdOutLogger) Debug(s string) {
	fmt.Print(s)
}

func (l stdOutLogger) Info(s string) {
	fmt.Print(s)
}

func (l stdOutLogger) Warn(s string) {
	fmt.Print(s)
}

func (l stdOutLogger) Error(s string) {
	fmt.Print(s)
}

func (l stdOutLogger) Fatal(s string) {
	fmt.Print(s)
}

func (l stdOutLogger) Debugf(s string, i ...interface{}) {
	fmt.Printf(s, i...)
}

func (l stdOutLogger) Infof(s string, i ...interface{}) {
	fmt.Printf(s, i...)
}

func (l stdOutLogger) Warnf(s string, i ...interface{}) {
	fmt.Printf(s, i...)
}

func (l stdOutLogger) Errorf(s string, i ...interface{}) {
	fmt.Printf(s, i...)
}

func (l stdOutLogger) Fatalf(s string, i ...interface{}) {
	fmt.Printf(s, i...)
}

var _ Logger = &stdOutLogger{}

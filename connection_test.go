package goamqp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestAddRequestResponseHandlerError(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	err := c.AddRequestResponseHandler("key", &incorrectRequestResponseMessageHandler{}).
		AddRequestResponseHandler("key", &RequestResponseMessageHandler{}).
		AddRequestResponseHandler("key", &RequestResponseMessageHandler{}).
		AddEventStreamListener("key", &TestIncomingMessageHandler{}).
		AddEventStreamListener("key", &incorrectProcessArgumentCount{}).
		AddEventStreamListener("key", &TestIncomingMessageHandler{}).(*connection).setup()
	assert.Error(t, err)
	assert.Len(t, strings.Split(err.Error(), "\n"), 5)
	assert.Contains(t, err.Error(), "handler goamqp.incorrectRequestResponseMessageHandler has incorrect number of return values. Expected 1, actual 0")
	assert.Contains(t, err.Error(), "routingkey key for queue events.topic.exchange.queue.svc already assigned to handler goamqp.TestIncomingMessageHandler, cannot assign goamqp.TestIncomingMessageHandler")
	assert.Contains(t, err.Error(), "routingkey key for queue svc.direct.exchange.request.queue already assigned to handler goamqp.RequestResponseMessageHandler, cannot assign goamqp.RequestResponseMessageHandler")
	assert.Contains(t, err.Error(), "handler goamqp.incorrectProcessArgumentCount has incorrect number of arguments, expected 1 but was 0")
}

func TestFailingSetupFunc(t *testing.T) {
	c := connection{setupFuncs: []setupFunc{func(channel amqpChannel) error { return nil }, func(channel amqpChannel) error { return fmt.Errorf("error message") }}}
	assert.EqualError(t, c.setup(), "setup function <gitlab.com/sparetimecoders/goamqp.TestFailingSetupFunc.func2> failed, error message")
}

func TestNewFromURL_InvalidURL(t *testing.T) {
	c := NewFromURL("test", "amqp://")
	assert.NotNil(t, c)
	assert.Equal(t, 1, len(c.(*connection).setupErrors))
	assert.EqualError(t, c.(*connection).setupErrors[0], "connection url is invalid, amqp://")
}

func TestNewFromURL_ValidURL(t *testing.T) {
	c := NewFromURL("test", "amqp://user:password@localhost:5672/")
	assert.NotNil(t, c)
	assert.Equal(t, 0, len(c.(*connection).setupErrors))
}

type TestIncomingMessageHandler struct{}

func (i TestIncomingMessageHandler) Process(m IncomingMessage) bool {
	return true
}

type IncomingMessage struct {
	URL string
}

type RequestResponseMessageHandler struct{}

func (i RequestResponseMessageHandler) Process(m IncomingMessage) (string, bool) {
	return "", true
}

package goamqp

import (
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

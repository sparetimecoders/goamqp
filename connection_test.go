package goamqp

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strings"
	"testing"
)

func TestSetupErrors(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	publisher := make(chan interface{})
	confirm := make(chan amqp.Confirmation)
	handler := &RequestResponseMessageHandler{}
	incomingHandler := &TestIncomingMessageHandler{}
	err := c.
		AddRequestResponseHandler("key", handler.Process, reflect.TypeOf(IncomingMessage{})).
		AddRequestResponseHandler("key", handler.Process, reflect.TypeOf(IncomingMessage{})).
		AddEventStreamListener("key", incomingHandler.Process, reflect.TypeOf(IncomingMessage{})).
		AddEventStreamListener("key", incomingHandler.Process, reflect.TypeOf(IncomingMessage{})).
		AddServicePublisher("target", "key", publisher, incomingHandler.Process, reflect.TypeOf(IncomingMessage{})).
		AddPublishNotify(confirm).
	(*connection).setup()
	assert.Error(t, err)
	errors := strings.Split(err.Error(), "\n")
	assert.Len(t, errors, 3)
	assert.Equal(t, "errors found during setup,", errors[0])
	assert.Equal(t, "\troutingkey key for queue svc.direct.exchange.request.queue already assigned to handler for type goamqp.IncomingMessage, cannot assign goamqp.IncomingMessage", errors[1])
	assert.Equal(t, "\troutingkey key for queue events.topic.exchange.queue.svc already assigned to handler for type goamqp.IncomingMessage, cannot assign goamqp.IncomingMessage", errors[2])
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

func TestMultiType(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)

	handler := &MultiTypeMessageHandler{}

	err := c.
		AddEventStreamListener("Msg", handler.Process, reflect.TypeOf(&IncomingMessage{})).(*connection).
		AddEventStreamListener("Other", handler.Process, reflect.TypeOf(&OtherMessage{})).(*connection).
		setup()

	assert.NoError(t, err)
}

type TestIncomingMessageHandler struct{}

func (i TestIncomingMessageHandler) Process(m interface{}) bool {
	return true
}

type IncomingMessage struct {
	URL string
}

type OtherMessage struct {
	Name string `json:"name"`
}

type RequestResponseMessageHandler struct{}

func (i RequestResponseMessageHandler) Process(m interface{}) (interface{}, bool) {
	return &OtherMessage{Name: "other"}, true
}

type MultiTypeMessageHandler struct{}

func (h MultiTypeMessageHandler) Process(m interface{}) bool {
	switch e := m.(type) {
	case IncomingMessage:
		fmt.Println(e.URL)
	case OtherMessage:
		fmt.Println(e.Name)
	}
	return true
}

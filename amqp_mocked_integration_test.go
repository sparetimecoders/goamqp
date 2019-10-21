// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package goamqp

import (
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type Consumer struct {
	queue     string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

type QueueDeclaration struct {
	name       string
	durable    bool
	autoDelete bool
	noWait     bool
	args       amqp.Table
}

type BindingDeclaration struct {
	queue    string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

type ExchangeDeclaration struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

type Publish struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

type TestMessage struct {
	Msg     string
	Success bool
}
type DelayedTestMessage struct {
	Msg string
}

func (DelayedTestMessage) TTL() time.Duration {
	return time.Second
}

type Ack struct {
	tag      uint64
	multiple bool
}
type Reject struct {
	tag     uint64
	requeue bool
}

type Nack struct {
	tag      uint64
	multiple bool
	requeue  bool
}

type MockAcknowledger struct {
	Acks    chan Ack
	Nacks   chan Nack
	Rejects chan Reject
}

type MockCloser struct {
	Called bool
}

func (c *MockCloser) Close() error {
	c.Called = true
	return nil
}
func (a *MockAcknowledger) Ack(tag uint64, multiple bool) error {
	a.Acks <- Ack{tag, multiple}
	return nil
}
func (a *MockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.Nacks <- Nack{tag, multiple, requeue}
	return nil
}
func (a *MockAcknowledger) Reject(tag uint64, requeue bool) error {
	a.Rejects <- Reject{tag, requeue}
	return nil
}

type MockAmqpChannel struct {
	ExchangeDeclarations []ExchangeDeclaration
	QueueDeclarations    []QueueDeclaration
	BindingDeclarations  []BindingDeclaration
	Consumers            []Consumer
	Published            chan Publish
	Delivery             chan amqp.Delivery
}

func (m *MockAmqpChannel) QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error {
	m.BindingDeclarations = append(m.BindingDeclarations, BindingDeclaration{queue, key, exchange, noWait, args})
	return nil
}

func (m *MockAmqpChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	m.Consumers = append(m.Consumers, Consumer{queue, consumer, autoAck, exclusive, noLocal, noWait, args})
	return m.Delivery, nil
}
func (m *MockAmqpChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	m.ExchangeDeclarations = append(m.ExchangeDeclarations, ExchangeDeclaration{name, kind, durable, autoDelete, internal, noWait, args})
	return nil
}
func (m *MockAmqpChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	m.Published <- Publish{exchange, key, mandatory, immediate, msg}
	return nil
}
func (m *MockAmqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	m.QueueDeclarations = append(m.QueueDeclarations, QueueDeclaration{name, durable, autoDelete, noWait, args})
	return amqp.Queue{}, nil
}
func TestCloseCallsUnderlyingCloseMethods(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(&channel)
	closer := MockCloser{}
	conn.connection = &closer
	conn.Close()
	assert.True(t, closer.Called)
}
func TestDelayedMessagingDisabled(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	c.config.DelayedMessage = false
	handler := &MockIncomingMessageHandler{}
	c.AddEventStreamListener("key", handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{}, kind: "topic"}, channel.ExchangeDeclarations[0])
}

func TestEventListenerSetup(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	handler := &MockIncomingMessageHandler{}
	c.AddEventStreamListener("key", handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "topic"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	assert.Equal(t, 1, len(channel.BindingDeclarations))
	assert.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	assert.Equal(t, 1, len(channel.Consumers))
	assert.Equal(t, Consumer{queue: "events.topic.exchange.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func TestEventListener(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	handler := &MockIncomingMessageHandler{Received: make(chan TestMessage, 2)}

	c.AddEventStreamListener("key", handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	acker := NewMockAcknowledger()

	delivery := amqp.Delivery{
		RoutingKey:   "key",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"test\",\"Success\":true}"),
		DeliveryTag:  uint64(123),
	}

	channel.Delivery <- delivery
	msg := <-handler.Received
	ack := <-acker.Acks

	assert.Equal(t, TestMessage{Msg: "test", Success: true}, msg)
	assert.Equal(t, Ack{uint64(123), false}, ack)
}

func TestEventListener_MultiType(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	handler := &MultiTypeMockMessageHandler{Received: make(chan interface{}, 2)}

	c.
		AddEventStreamListener("key", handler.Process, reflect.TypeOf(TestMessage{})).
		AddEventStreamListener("other", handler.Process, reflect.TypeOf(DelayedTestMessage{})).(*connection).setup()
	acker := NewMockAcknowledger()

	delivery := amqp.Delivery{
		RoutingKey:   "other",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"test\",\"Success\":true}"),
		DeliveryTag:  uint64(123),
	}

	channel.Delivery <- delivery
	msg := <-handler.Received
	ack := <-acker.Acks

	assert.Equal(t, &DelayedTestMessage{Msg: "test"}, msg)
	assert.Equal(t, Ack{uint64(123), false}, ack)

	delivery = amqp.Delivery{
		RoutingKey:   "key",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"test\",\"Success\":true}"),
		DeliveryTag:  uint64(123),
	}

	channel.Delivery <- delivery
	msg = <-handler.Received
	ack = <-acker.Acks

	assert.Equal(t, &TestMessage{Msg: "test", Success: true}, msg)
	assert.Equal(t, Ack{uint64(123), false}, ack)
}

func TestUnhandledEventsGetsRejected(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	handler := &MockIncomingMessageHandler{Received: make(chan TestMessage, 2)}

	c.AddEventStreamListener("key", handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	acker := NewMockAcknowledger()

	delivery := amqp.Delivery{
		RoutingKey:   "keymissing",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"test\",\"Success\":true}"),
		DeliveryTag:  uint64(123),
	}

	channel.Delivery <- delivery

	reject := <-acker.Rejects

	assert.Equal(t, Reject{uint64(123), false}, reject)
}

func TestEventPublisher(t *testing.T) {

	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	p := make(chan interface{}, 2)
	c.AddEventStreamPublisher("key", p).(*connection).setup()
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "topic"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 0, len(channel.QueueDeclarations))
	assert.Equal(t, 0, len(channel.BindingDeclarations))

	p <- TestMessage{"test", true}

	published := <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "events.topic.exchange", published.exchange)
	assert.Equal(t, false, published.immediate)
	assert.Equal(t, false, published.mandatory)

	assert.Equal(t, uint8(2), published.msg.DeliveryMode)
	assert.Equal(t, "application/json", published.msg.ContentType)
	assert.Equal(t, 1, len(published.msg.Headers))
	assert.Equal(t, "svc", published.msg.Headers["service"])
	assert.Equal(t, "", published.msg.ReplyTo)
	assert.Equal(t, "{\"Msg\":\"test\",\"Success\":true}", string(published.msg.Body))

	p <- DelayedTestMessage{"test"}
	published = <-channel.Published

	assert.Equal(t, 2, len(published.msg.Headers))
	assert.Equal(t, "1000", published.msg.Headers["x-delay"])
	assert.Equal(t, "svc", published.msg.Headers["service"])

}

func TestServicePublisher(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)

	p := make(chan interface{}, 2)

	c.AddServicePublisher("svc", "key", p, nil, nil).(*connection).setup()
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	p <- TestMessage{"test", true}
	published := <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "svc.direct.exchange.request", published.exchange)
}

func TestServicePublisherWithHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)

	p := make(chan interface{}, 2)

	handler := &MockIncomingMessageHandler{}
	c.AddServicePublisher("svc", "key", p, handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[1])
	assert.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "headers"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	p <- TestMessage{"test", true}
	published := <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "svc.direct.exchange.request", published.exchange)
}

type MockIncomingMessageHandler struct {
	Received chan TestMessage
}

func (m *MockIncomingMessageHandler) Process(msg interface{}) bool {
	if x, ok := msg.(*TestMessage); ok {
		m.Received <- *x
		return x.Success
	}
	return false
}

type MultiTypeMockMessageHandler struct {
	Received chan interface{}
}

func (x *MultiTypeMockMessageHandler) Process(msg interface{}) bool {
	x.Received <- msg
	return true
}

type Response struct {
	Ok bool
}

type MockRequestResponseHandler struct {
	Received chan TestMessage
}

func (m *MockRequestResponseHandler) Process(msg interface{}) (interface{}, bool) {
	if x, ok := msg.(*TestMessage); ok {
		m.Received <- *x
		return &Response{Ok:true}, x.Success
	}
	return &Response{Ok:false}, false
}

func TestRequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	c := mockConnection(&channel)
	handler := &MockRequestResponseHandler{Received: make(chan TestMessage, 2)}
	c.AddRequestResponseHandler("key", handler.Process, reflect.TypeOf(TestMessage{})).(*connection).setup()
	assert.Equal(t, 2, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "headers"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[1])

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	assert.Equal(t, 1, len(channel.BindingDeclarations))
	assert.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	acker := NewMockAcknowledger()

	delivery := amqp.Delivery{
		RoutingKey:   "key",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"test\",\"Success\":true}"),
		DeliveryTag:  uint64(123),
	}

	channel.Delivery <- delivery
	msg := <-handler.Received
	ack := <-acker.Acks
	response := <-channel.Published

	assert.Equal(t, "test", msg.Msg)
	assert.Equal(t, false, ack.multiple)
	assert.Equal(t, uint64(123), ack.tag)
	assert.Equal(t, "{\"Ok\":true}", string(response.msg.Body))

	delivery = amqp.Delivery{
		RoutingKey:   "key",
		Acknowledger: &acker,
		Body:         []byte("{\"Msg\":\"failed\",\"Success\":false}"),
		DeliveryTag:  uint64(1),
	}
	channel.Delivery <- delivery
	msg = <-handler.Received
	nack := <-acker.Nacks

	assert.Equal(t, "failed", msg.Msg)
	assert.Equal(t, false, nack.multiple)
	assert.Equal(t, true, nack.requeue)
	assert.Equal(t, uint64(1), nack.tag)
}

func NewMockAmqpChannel() MockAmqpChannel {
	return MockAmqpChannel{
		Published: make(chan Publish, 2),
		Delivery:  make(chan amqp.Delivery, 2),
	}
}

func NewMockAcknowledger() MockAcknowledger {
	return MockAcknowledger{
		Acks:    make(chan Ack, 2),
		Nacks:   make(chan Nack, 2),
		Rejects: make(chan Reject, 2),
	}
}

func mockConnection(channel *MockAmqpChannel) *connection {
	conn := New("svc", AmqpConfig{DelayedMessage: true})
	conn.(*connection).channel = channel

	return conn.(*connection)
}

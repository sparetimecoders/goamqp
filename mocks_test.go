package goamqp

import (
	"errors"
	"github.com/streadway/amqp"
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
	ExchangeDeclarations     []ExchangeDeclaration
	QueueDeclarations        []QueueDeclaration
	BindingDeclarations      []BindingDeclaration
	Consumers                []Consumer
	Published                chan Publish
	Delivery                 chan amqp.Delivery
	Confirms                 *chan amqp.Confirmation
	ExchangeDeclarationError *error
	QueueDeclarationError    *error
	NotifyCloseCalled        bool
	ConfirmCalled            bool
}

func (m *MockAmqpChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	panic("implement me")
}

func (m *MockAmqpChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	m.Confirms = &confirm
	return confirm
}

func (m *MockAmqpChannel) Confirm(noWait bool) error {
	m.ConfirmCalled = true
	return nil
}

func (m *MockAmqpChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	m.NotifyCloseCalled = true
	return nil
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
	if m.ExchangeDeclarationError != nil {
		return *m.ExchangeDeclarationError
	}

	m.ExchangeDeclarations = append(m.ExchangeDeclarations, ExchangeDeclaration{name, kind, durable, autoDelete, internal, noWait, args})
	return nil
}
func (m *MockAmqpChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if key == "failed" {
		return errors.New("failed")
	}
	m.Published <- Publish{exchange, key, mandatory, immediate, msg}
	if m.Confirms != nil {
		*m.Confirms <- amqp.Confirmation{
			DeliveryTag: 1,
			Ack:         true,
		}
	}
	return nil
}
func (m *MockAmqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if m.QueueDeclarationError != nil {
		return amqp.Queue{}, *m.QueueDeclarationError
	}

	m.QueueDeclarations = append(m.QueueDeclarations, QueueDeclaration{name, durable, autoDelete, noWait, args})
	return amqp.Queue{}, nil
}

type MockAmqpConnection struct {
	CloseCalled      bool
	ChannelConnected bool
}

func (m *MockAmqpConnection) Close() error {
	m.CloseCalled = true
	return nil
}

func (m *MockAmqpConnection) Channel() (*amqp.Channel, error) {
	if m.ChannelConnected {
		return &amqp.Channel{}, nil
	} else {
		return nil, errors.New("failed to get channel")
	}
}

func NewMockAmqpChannel() *MockAmqpChannel {
	return &MockAmqpChannel{
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

var _ amqpConnection = &MockAmqpConnection{}
var _ AmqpChannel = &MockAmqpChannel{}

func mockConnection(channel *MockAmqpChannel) *Connection {
	c := newConnection("svc", AmqpConfig{DelayedMessage: true})
	c.Channel = channel
	c.connection = &MockAmqpConnection{}
	return c
}

type badRand struct{}

func (r badRand) Read(buf []byte) (int, error) {
	for i := range buf {
		buf[i] = byte(i)
	}
	return len(buf), nil
}

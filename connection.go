// MIT License
//
// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package goamqp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Setup is a setup function that takes a Connection and use it to set up AMQP
// An example is to create exchanges and queues
type Setup func(conn *Connection) error

// HandlerFunc is used to process an incoming message
// If processing fails, an error should be returned and the message will be re-queued
// The optional response is used automatically when setting up a RequestResponseHandler, otherwise ignored
type HandlerFunc func(msg any, headers Headers) (response any, err error)

// Route defines the routing key to be used for a message type
type Route struct {
	Type any
	Key  string
}

// Connection is a wrapper around the actual amqp.Connection and amqp.Channel
type Connection struct {
	started     bool
	serviceName string
	amqpUri     amqp.URI
	connection  amqpConnection
	channel     AmqpChannel
	handlers    map[queueRoutingKey]messageHandlerInvoker
	// messageLogger defaults to noOpMessageLogger, can be overridden with UseMessageLogger
	messageLogger MessageLogger
	// errorLogF defaults to noOpLogger, can be overridden with UseLogger
	errorLogF errorLogf
}

// ServiceResponsePublisher represents the function that is called to publish a response
type ServiceResponsePublisher func(targetService, routingKey string, msg any) error

// QueueBindingConfig is a wrapper around the actual amqp queue configuration
type QueueBindingConfig struct {
	routingKey   string
	handler      HandlerFunc
	eventType    eventType
	queueName    string
	exchangeName string
	kind         kind
	headers      amqp.Table
}

// QueueBindingConfigSetup is a setup function that takes a QueueBindingConfig and provide custom changes to the
// configuration
type QueueBindingConfigSetup func(config *QueueBindingConfig) error

// AddQueueNameSuffix appends the provided suffix to the queue name
// Useful when multiple consumers are needed for a routing key in the same service
func AddQueueNameSuffix(suffix string) QueueBindingConfigSetup {
	return func(config *QueueBindingConfig) error {
		if suffix == "" {
			return ErrEmptySuffix
		}
		config.queueName = fmt.Sprintf("%s-%s", config.queueName, suffix)
		return nil
	}
}

var (
	// ErrEmptySuffix returned when an empty suffix is passed
	ErrEmptySuffix = fmt.Errorf("empty queue suffix not allowed")
	// ErrAlreadyStarted returned when Start is called multiple times
	ErrAlreadyStarted = fmt.Errorf("already started")
	// ErrIllegalEventType is returned when an illegal type is passed
	ErrIllegalEventType = fmt.Errorf("passing reflect.TypeOf event types is not allowed")
	// ErrNilLogger is returned if nil is passed as a logger func
	ErrNilLogger = errors.New("cannot use nil as logger func")
)

// NewFromURL creates a new Connection from an URL
func NewFromURL(serviceName string, amqpURL string) (*Connection, error) {
	uri, err := amqp.ParseURI(amqpURL)
	if err != nil {
		return nil, err
	}
	return newConnection(serviceName, uri), nil
}

// Must is a helper that wraps a call to a function returning (*T, error)
// and panics if the error is non-nil. It is intended for use in variable
// initializations such as
// var c = goamqp.Must(goamqp.NewFromURL("service", "amqp://"))
func Must[T any](t *T, err error) *T {
	if err != nil {
		panic(err)
	}
	return t
}

// CloseListener receives a callback when the AMQP Channel gets closed
func CloseListener(e chan error) Setup {
	return func(c *Connection) error {
		temp := make(chan *amqp.Error)
		go func() {
			for {
				if ev := <-temp; ev != nil {
					e <- errors.New(ev.Error())
				}
			}
		}()
		c.channel.NotifyClose(temp)
		return nil
	}
}

// UseLogger allows an errorLogf to be used to log errors during processing of messages
func UseLogger(logger errorLogf) Setup {
	return func(c *Connection) error {
		if logger == nil {
			return ErrNilLogger
		}
		c.errorLogF = logger
		return nil
	}
}

// UseMessageLogger allows a MessageLogger to be used when log in/outgoing messages
func UseMessageLogger(logger MessageLogger) Setup {
	return func(c *Connection) error {
		if logger == nil {
			return ErrNilLogger
		}
		c.messageLogger = logger
		return nil
	}
}

// WithPrefetchLimit configures the number of messages to prefetch from the server.
// To get round-robin behavior between consumers consuming from the same queue on
// different connections, set the prefetch count to 1, and the next available
// message on the server will be delivered to the next available consumer.
// If your consumer work time is reasonably consistent and not much greater
// than two times your network round trip time, you will see significant
// throughput improvements starting with a prefetch count of 2 or slightly
// greater, as described by benchmarks on RabbitMQ.
//
// http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
func WithPrefetchLimit(limit int) Setup {
	return func(conn *Connection) error {
		return conn.channel.Qos(limit, 0, true)
	}
}

// TransientEventStreamConsumer sets up an event stream consumer that will clean up resources when the
// connection is closed.
// For a durable queue, use the EventStreamConsumer function instead.
func TransientEventStreamConsumer(routingKey string, handler HandlerFunc, eventType any) Setup {
	return func(c *Connection) error {
		eventTyp, err := getEventType(eventType)
		if err != nil {
			return err
		}
		queueName := serviceEventRandomQueueName(c.serviceName)
		exchangeName := eventsExchangeName()
		if err := c.addHandler(queueName, routingKey, eventTyp, messageHandlerInvoker{
			msgHandler: handler,
			queueRoutingKey: queueRoutingKey{
				Queue:      queueName,
				RoutingKey: routingKey,
			},
			eventType: eventTyp,
		}); err != nil {
			return err
		}

		if err := c.exchangeDeclare(c.channel, exchangeName, kindTopic); err != nil {
			return err
		}
		if err := transientQueueDeclare(c.channel, queueName); err != nil {
			return err
		}
		return c.channel.QueueBind(queueName, routingKey, exchangeName, false, amqp.Table{})
	}
}

// EventStreamConsumer sets up ap a durable, persistent event stream consumer.
// For a transient queue, use the TransientEventStreamConsumer function instead.
func EventStreamConsumer(routingKey string, handler HandlerFunc, eventType any, opts ...QueueBindingConfigSetup) Setup {
	return func(c *Connection) error {
		eventTyp, err := getEventType(eventType)
		if err != nil {
			return err
		}
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventTyp,
			queueName:    serviceEventQueueName(c.serviceName),
			exchangeName: eventsExchangeName(),
			kind:         kindTopic,
			headers:      amqp.Table{},
		}
		for _, f := range opts {
			if err := f(config); err != nil {
				return fmt.Errorf("queuebinding setup function <%s> failed, %v", getQueueBindingConfigSetupFuncName(f), err)
			}
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// EventStreamPublisher sets up an event stream publisher
func EventStreamPublisher(publisher *Publisher) Setup {
	return func(c *Connection) error {
		if err := c.exchangeDeclare(c.channel, eventsExchangeName(), kindTopic); err != nil {
			return errors.Wrapf(err, "failed to declare exchange %s", eventsExchangeName())
		}
		publisher.connection = c
		if err := publisher.setDefaultHeaders(c.serviceName); err != nil {
			return err
		}
		publisher.exchange = eventsExchangeName()
		return nil
	}
}

// QueuePublisher sets up a publisher that will send events to a specific queue instead of using the exchange,
// so called Sender-Selected distribution
// https://www.rabbitmq.com/sender-selected.html#:~:text=The%20RabbitMQ%20broker%20treats%20the,key%20if%20they%20are%20present.
func QueuePublisher(publisher *Publisher, destinationQueueName string) Setup {
	return func(c *Connection) error {
		publisher.connection = c
		if err := publisher.setDefaultHeaders(c.serviceName,
			Header{Key: "CC", Value: []any{destinationQueueName}},
		); err != nil {
			return err
		}
		publisher.exchange = ""
		return nil
	}
}

// ServiceResponseConsumer is a specialization of EventStreamConsumer
// It sets up ap a durable, persistent consumer (exchange->queue) for responses from targetService
func ServiceResponseConsumer(targetService, routingKey string, handler HandlerFunc, eventType any) Setup {
	return func(c *Connection) error {
		eventTyp, err := getEventType(eventType)
		if err != nil {
			return err
		}
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventTyp,
			queueName:    serviceResponseQueueName(targetService, c.serviceName),
			exchangeName: serviceResponseExchangeName(targetService),
			kind:         kindHeaders,
			headers:      amqp.Table{headerService: c.serviceName},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// ServiceRequestConsumer is a specialization of EventStreamConsumer
// It sets up ap a durable, persistent consumer (exchange->queue) for message to the service owning the Connection
func ServiceRequestConsumer(routingKey string, handler HandlerFunc, eventType any) Setup {
	return func(c *Connection) error {
		eventTyp, err := getEventType(eventType)
		if err != nil {
			return err
		}
		resExchangeName := serviceResponseExchangeName(c.serviceName)
		if err := c.exchangeDeclare(c.channel, resExchangeName, kindHeaders); err != nil {
			return errors.Wrapf(err, "failed to create exchange %s", resExchangeName)
		}

		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventTyp,
			queueName:    serviceRequestQueueName(c.serviceName),
			exchangeName: serviceRequestExchangeName(c.serviceName),
			kind:         kindDirect,
			headers:      amqp.Table{},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// ServicePublisher sets up ap a publisher, that sends messages to the targetService
func ServicePublisher(targetService string, publisher *Publisher) Setup {
	return func(c *Connection) error {
		reqExchangeName := serviceRequestExchangeName(targetService)
		publisher.connection = c
		if err := publisher.setDefaultHeaders(c.serviceName); err != nil {
			return err
		}
		publisher.exchange = reqExchangeName
		if err := c.exchangeDeclare(c.channel, reqExchangeName, kindDirect); err != nil {
			return err
		}
		return nil
	}
}

// RequestResponseHandler is a convenience func to set up ServiceRequestConsumer and combines it with
// PublishServiceResponse
func RequestResponseHandler(routingKey string, handler HandlerFunc, eventType any) Setup {
	return func(c *Connection) error {
		responseHandlerWrapper := responseWrapper(handler, routingKey, c.PublishServiceResponse)
		return ServiceRequestConsumer(routingKey, responseHandlerWrapper, eventType)(c)
	}
}

// PublishServiceResponse sends a message to targetService as a handler response
func (c *Connection) PublishServiceResponse(targetService, routingKey string, msg any) error {
	return c.publishMessage(msg, routingKey, serviceResponseExchangeName(c.serviceName), amqp.Table{headerService: targetService})
}

// PublishNotify see amqp.Channel.Confirm
func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.channel.NotifyPublish(confirm)
		return c.channel.Confirm(false)
	}
}

// Start setups the amqp queues and exchanges defined by opts
func (c *Connection) Start(ctx context.Context, opts ...Setup) error {
	if c.started {
		return ErrAlreadyStarted
	}
	c.messageLogger = noOpMessageLogger()
	c.errorLogF = noOpLogger
	if c.channel == nil {
		err := c.connectToAmqpURL()
		if err != nil {
			return err
		}
	}

	if err := c.channel.Qos(20, 0, true); err != nil {
		return err
	}

	for _, f := range opts {
		if err := f(c); err != nil {
			return fmt.Errorf("setup function <%s> failed, %v", getSetupFuncName(f), err)
		}
	}

	if err := c.setup(); err != nil {
		return err
	}

	c.started = true
	return nil
}

// Close closes the amqp connection, see amqp.Connection.Close
func (c *Connection) Close() error {
	if !c.started {
		return nil
	}
	return c.connection.Close()
}

// AmqpChannel wraps the amqp.Channel to allow for mocking
type AmqpChannel interface {
	QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Confirm(noWait bool) error
	// Qos controls how many messages or how many bytes the server will try to keep on
	// the network for consumers before receiving delivery acks.  The intent of Qos is
	// to make sure the network buffers stay full between the server and client.
	// If your consumer work time is reasonably consistent and not much greater
	// than two times your network round trip time, you will see significant
	// throughput improvements starting with a prefetch count of 2 or slightly
	// greater as described by benchmarks on RabbitMQ.
	//
	// http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
	// The default prefetchCount is 20 (and global true) and can be overridden with WithPrefetchLimit
	Qos(prefetchCount, prefetchSize int, global bool) error
}

var _ AmqpChannel = &amqp.Channel{}

type amqpConnection interface {
	io.Closer
	Channel() (*amqp.Channel, error)
}

func dialConfig(url string, cfg amqp.Config) (amqpConnection, error) {
	return amqp.DialConfig(url, cfg)
}

var dialAmqp = dialConfig

func amqpVersion() string {
	// NOTE: this doesn't work outside of a build, se we can't really test it
	if x, ok := debug.ReadBuildInfo(); ok {
		for _, y := range x.Deps {
			if y.Path == "github.com/sparetimecoders/goamqp" {
				return y.Version
			}
		}
	}
	return "_unknown_"
}

func responseWrapper(handler HandlerFunc, routingKey string, publisher ServiceResponsePublisher) HandlerFunc {
	return func(msg any, headers Headers) (response any, err error) {
		resp, err := handler(msg, headers)
		if err != nil {
			return nil, errors.Wrap(err, "failed to process message")
		}
		if resp != nil {
			service, err := sendingService(headers)
			if err != nil {
				return nil, errors.Wrap(err, "failed to extract service name")
			}
			err = publisher(service, routingKey, resp)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to publish response")
			}
			return resp, nil
		}
		return nil, nil
	}
}

func consume(channel AmqpChannel, queue string) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		amqp.Table{},
	)
}

func queueDeclare(channel AmqpChannel, name string) error {
	_, err := channel.QueueDeclare(name,
		true,
		false,
		false,
		false,
		queueDeclareExpiration,
	)
	return err
}

func transientQueueDeclare(channel AmqpChannel, name string) error {
	_, err := channel.QueueDeclare(name,
		false,
		true,
		false,
		false,
		queueDeclareExpiration,
	)
	return err
}

func amqpConfig(serviceName string) amqp.Config {
	config := amqp.Config{
		Properties: amqp.NewConnectionProperties(),
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
	}
	config.Properties.SetClientConnectionName(fmt.Sprintf("%s#%+v#@%s", serviceName, amqpVersion(), hostName()))
	return config
}

func hostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "_unknown_"
	}
	return hostname
}

func (c *Connection) connectToAmqpURL() error {
	cfg := amqpConfig(c.serviceName)

	conn, err := dialAmqp(c.amqpUri.String(), cfg)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	c.channel = ch
	c.connection = conn
	return nil
}

func (c *Connection) exchangeDeclare(channel AmqpChannel, name string, kind kind) error {
	args := amqp.Table{}

	return channel.ExchangeDeclare(
		name,
		string(kind),
		true,
		false,
		false,
		false,
		args,
	)
}

func (c *Connection) addHandler(queueName, routingKey string, eventType eventType, mHI messageHandlerInvoker) error {
	uniqueKey := queueRoutingKey{Queue: queueName, RoutingKey: routingKey}

	if existing, exist := c.handlers[uniqueKey]; exist {
		return fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s, consider using AddQueueNameSuffix", routingKey, queueName, existing.eventType, eventType)
	}
	c.handlers[uniqueKey] = mHI

	return nil
}

func (c *Connection) handleMessage(d amqp.Delivery, handler HandlerFunc, eventType eventType, routingKey string) {
	message, err := c.parseMessage(d.Body, eventType, routingKey)
	if err != nil {
		_ = d.Reject(false)
	} else {
		if _, err := handler(message, headers(d.Headers)); err == nil {
			_ = d.Ack(false)
		} else {
			c.errorLogF("failed to process message %s", err)
			_ = d.Nack(false, true)
		}
	}
}

func (c *Connection) parseMessage(jsonContent []byte, eventType eventType, routingKey string) (any, error) {
	c.messageLogger(jsonContent, eventType, routingKey, false)
	target := reflect.New(eventType).Interface()
	if err := json.Unmarshal(jsonContent, &target); err != nil {
		return nil, err
	}

	return target, nil
}

func (c *Connection) publishMessage(msg any, routingKey, exchangeName string, headers amqp.Table) error {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.messageLogger(jsonBytes, reflect.TypeOf(msg), routingKey, true)

	publishing := amqp.Publishing{
		Body:         jsonBytes,
		ContentType:  contentType,
		DeliveryMode: 2,
		Headers:      headers,
	}

	return c.channel.Publish(exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)
}

func (c *Connection) divertToMessageHandlers(deliveries <-chan amqp.Delivery, handlers map[string]messageHandlerInvoker) {
	for d := range deliveries {
		if h, ok := handlers[d.RoutingKey]; ok {
			c.handleMessage(d, h.msgHandler, h.eventType, d.RoutingKey)
		} else {
			// Unhandled message, drop it
			_ = d.Reject(false)
		}
	}
}

func (c *Connection) messageHandlerBindQueueToExchange(cfg *QueueBindingConfig) error {
	if err := c.addHandler(cfg.queueName, cfg.routingKey, cfg.eventType, messageHandlerInvoker{
		msgHandler: cfg.handler,
		queueRoutingKey: queueRoutingKey{
			Queue:      cfg.queueName,
			RoutingKey: cfg.routingKey,
		},
		eventType: cfg.eventType,
	}); err != nil {
		return err
	}

	if err := c.exchangeDeclare(c.channel, cfg.exchangeName, cfg.kind); err != nil {
		return err
	}
	if err := queueDeclare(c.channel, cfg.queueName); err != nil {
		return err
	}
	return c.channel.QueueBind(cfg.queueName, cfg.routingKey, cfg.exchangeName, false, cfg.headers)
}

type kind string

const kindDirect = "direct"
const kindHeaders = "headers"
const kindTopic = "topic"

const headerService = "service"
const headerExpires = "x-expires"

const contentType = "application/json"

var deleteQueueAfter = 5 * 24 * time.Hour
var queueDeclareExpiration = amqp.Table{headerExpires: int(deleteQueueAfter.Seconds() * 1000)}

func newConnection(serviceName string, uri amqp.URI) *Connection {
	return &Connection{
		serviceName: serviceName,
		amqpUri:     uri,
		handlers:    make(map[queueRoutingKey]messageHandlerInvoker),
	}
}

func (c *Connection) setup() error {
	queues := make(map[string][]messageHandlerInvoker)

	for qr, h := range c.handlers {
		queues[qr.Queue] = append(queues[qr.Queue], h)
	}

	for q, h := range queues {
		queueHandlers := make(map[string]messageHandlerInvoker)
		for _, kh := range h {
			queueHandlers[kh.RoutingKey] = kh
		}
		consumer, err := consume(c.channel, q)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", q, err)
		}
		go c.divertToMessageHandlers(consumer, queueHandlers)
	}
	return nil
}

func getEventType(eventType any) (eventType, error) {
	if _, ok := eventType.(reflect.Type); ok {
		return nil, ErrIllegalEventType
	}
	return reflect.TypeOf(eventType), nil
}

type eventType reflect.Type

type routes map[reflect.Type]string

type queueRoutingKey struct {
	Queue      string
	RoutingKey string
}

type messageHandlerInvoker struct {
	msgHandler HandlerFunc
	queueRoutingKey
	eventType eventType
}

// getSetupFuncName returns the name of the Setup function
func getSetupFuncName(f Setup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// getQueueBindingConfigSetupFuncName returns the name of the QueueBindingConfigSetup function
func getQueueBindingConfigSetupFuncName(f QueueBindingConfigSetup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// sendingService returns the name of the service that produced the message
// Can be used to send a handlerResponse, see PublishServiceResponse
func sendingService(headers Headers) (string, error) {
	if h, exist := headers[headerService]; exist {
		switch v := h.(type) {
		case string:
			return v, nil
		}
	}
	return "", errors.New("no service found")
}

// MIT License
//
// Copyright (c) 2024 sparetimecoders
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
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is a wrapper around the actual amqp.Connection and amqp.Channel
type Connection struct {
	started     bool
	serviceName string
	amqpUri     amqp.URI
	connection  amqpConnection
	// TODO One channel per queue/consumer
	channel        AmqpChannel
	queueHandlers  *QueueHandlers
	typeToKey      map[reflect.Type]string
	keyToType      map[string]reflect.Type
	notificationCh chan<- Notification
}

// ServiceResponsePublisher represents the function that is called to publish a response
type ServiceResponsePublisher[T any] func(ctx context.Context, targetService, routingKey string, msg T) error

// QueueBindingConfig is a wrapper around the actual amqp queue configuration
type QueueBindingConfig struct {
	routingKey   string
	handler      wrappedHandler
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
	// ErrRecoverable will not be logged during message processing
	ErrRecoverable = errors.New("recoverable error")
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

// PublishServiceResponse sends a message to targetService as a handler response
func (c *Connection) PublishServiceResponse(ctx context.Context, targetService, routingKey string, msg any) error {
	return c.publishMessage(ctx, msg, routingKey, serviceResponseExchangeName(c.serviceName), amqp.Table{headerService: targetService})
}

func (c *Connection) URI() amqp.URI {
	return c.amqpUri
}

// Start setups the amqp queues and exchanges defined by opts
func (c *Connection) Start(ctx context.Context, opts ...Setup) error {
	if c.started {
		return ErrAlreadyStarted
	}
	// TODO Multiple channels
	if c.channel == nil {
		err := c.connectToAmqpURL()
		if err != nil {
			return err
		}
	}

	// TODO Qos from opt (per queue?)
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
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
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

func version() string {
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

func responseWrapper[T, R any](handler RequestResponseEventHandler[T, R], routingKey string, publisher ServiceResponsePublisher[R]) EventHandler[T] {
	return func(ctx context.Context, event ConsumableEvent[T]) (err error) {
		resp, err := handler(ctx, event)
		if err != nil {
			return fmt.Errorf("failed to process message, %w", err)
		}
		service, err := sendingService(event.DeliveryInfo)
		if err != nil {
			return fmt.Errorf("failed to extract service name, %w", err)
		}
		// TODO Handle response with type R
		err = publisher(ctx, service, routingKey, resp)
		if err != nil {
			return fmt.Errorf("failed to publish response, %w", err)
		}
		return nil
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
		true,
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
	config.Properties.SetClientConnectionName(fmt.Sprintf("%s#%+v#@%s", serviceName, version(), hostName()))
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
	// TODO Multiple channels
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

func (c *Connection) addHandler(queueName, routingKey string, handler wrappedHandler) error {
	return c.queueHandlers.Add(queueName, routingKey, handler)
}

func (c *Connection) publishMessage(ctx context.Context, msg any, routingKey, exchangeName string, headers amqp.Table) error {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		Body:         jsonBytes,
		ContentType:  contentType,
		DeliveryMode: 2,
		Headers:      injectToHeaders(ctx, headers),
	}
	return c.channel.PublishWithContext(ctx, exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)
}

func getDeliveryInfo(queueName string, delivery amqp.Delivery) DeliveryInfo {
	deliveryInfo := DeliveryInfo{
		Queue:      queueName,
		Exchange:   delivery.Exchange,
		RoutingKey: delivery.RoutingKey,
		Headers:    Headers(delivery.Headers),
	}
	return deliveryInfo
}

func (c *Connection) divertToMessageHandlers(deliveries <-chan amqp.Delivery, queue QueueWithHandlers) {
	for delivery := range deliveries {
		startTime := time.Now()
		deliveryInfo := getDeliveryInfo(queue.Name, delivery)
		EventReceived(queue.Name, deliveryInfo.RoutingKey)

		// Establish which handler is invoked
		handler, ok := queue.Handlers.get(deliveryInfo.RoutingKey)
		if !ok {
			_ = delivery.Reject(false)
			EventWithoutHandler(queue.Name, deliveryInfo.RoutingKey)
			continue
		}

		uevt := unmarshalEvent{DeliveryInfo: deliveryInfo, Payload: delivery.Body}
		tracingCtx := extractToContext(delivery.Headers)
		// TODO Handle response
		if err := handler(tracingCtx, uevt); err != nil {
			elapsed := time.Since(startTime).Milliseconds()
			notifyEventHandlerFailed(c.notificationCh, deliveryInfo.RoutingKey, elapsed, err)
			if errors.Is(err, ErrParseJSON) {
				EventNotParsable(queue.Name, deliveryInfo.RoutingKey)
				_ = delivery.Nack(false, false)
			} else {
				_ = delivery.Nack(false, true)
			}
			EventNack(queue.Name, deliveryInfo.RoutingKey, elapsed)
			continue
		}

		elapsed := time.Since(startTime).Milliseconds()
		notifyEventHandlerSucceed(c.notificationCh, deliveryInfo.RoutingKey, elapsed)
		_ = delivery.Ack(false)
		EventAck(queue.Name, deliveryInfo.RoutingKey, elapsed)

	}
}

func (c *Connection) messageHandlerBindQueueToExchange(cfg *QueueBindingConfig) error {
	if err := c.addHandler(cfg.queueName, cfg.routingKey, cfg.handler); err != nil {
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

const (
	kindDirect  = "direct"
	kindHeaders = "headers"
	kindTopic   = "topic"
)

const (
	headerService = "service"
	headerExpires = "x-expires"
)

const contentType = "application/json"

var (
	deleteQueueAfter       = 5 * 24 * time.Hour
	queueDeclareExpiration = amqp.Table{headerExpires: int(deleteQueueAfter.Seconds() * 1000)}
)

func newConnection(serviceName string, uri amqp.URI) *Connection {
	return &Connection{
		serviceName:   serviceName,
		amqpUri:       uri,
		queueHandlers: &QueueHandlers{},
		keyToType:     make(map[string]reflect.Type),
		typeToKey:     make(map[reflect.Type]string),
	}
}

func (c *Connection) setup() error {
	for _, queue := range c.queueHandlers.Queues() {
		// TODO one channel per queue`
		consumer, err := consume(c.channel, queue.Name)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", queue.Name, err)
		}
		go c.divertToMessageHandlers(consumer, queue)
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
func sendingService(di DeliveryInfo) (string, error) {
	if h, exist := di.Headers[headerService]; exist {
		switch v := h.(type) {
		case string:
			return v, nil
		}
	}
	return "", errors.New("no service found")
}

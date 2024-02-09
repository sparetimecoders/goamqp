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
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Connection is a wrapper around the actual amqp.Connection and amqp.Channel
type Connection struct {
	started        bool
	serviceName    string
	amqpUri        amqp.URI
	connection     amqpConnection
	channel        AmqpChannel
	queueHandlers  *queueHandlers
	typeToKey      TypeToRoutingKey
	keyToType      RoutingKeyToType
	notificationCh chan<- Notification
}

// ServiceResponsePublisher represents the function that is called to publish a response
type ServiceResponsePublisher[T any] func(ctx context.Context, targetService, routingKey string, event T) error

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

// PublishServiceResponse sends a message to targetService as a handler response
func (c *Connection) PublishServiceResponse(ctx context.Context, targetService, routingKey string, msg any) error {
	return publishMessage(ctx, c.channel, msg, routingKey, serviceResponseExchangeName(c.serviceName), amqp.Table{headerService: targetService})
}

func (c *Connection) URI() amqp.URI {
	return c.amqpUri
}

// Start setups the amqp queues and exchanges defined by opts
func (c *Connection) Start(ctx context.Context, opts ...Setup) error {
	if c.started {
		return ErrAlreadyStarted
	}
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

func consume(channel AmqpChannel, queue string) (<-chan amqp.Delivery, error) {
	return channel.Consume(queue, "", false, false, false, false, nil)
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
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	c.channel = ch
	c.connection = conn
	return nil
}

func (c *Connection) addHandler(queueName, routingKey string, handler wrappedHandler) error {
	return c.queueHandlers.add(queueName, routingKey, handler)
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

func (c *Connection) divertToMessageHandlers(deliveries <-chan amqp.Delivery, queue queueWithHandlers) {
	for delivery := range deliveries {
		deliveryInfo := getDeliveryInfo(queue.Name, delivery)
		eventReceived(queue.Name, deliveryInfo.RoutingKey)

		// Establish which handler is invoked
		handler, ok := queue.Handlers.get(deliveryInfo.RoutingKey)
		if !ok {
			eventWithoutHandler(queue.Name, deliveryInfo.RoutingKey)
			_ = delivery.Reject(false)
			continue
		}
		c.handleDelivery(handler, delivery, deliveryInfo)
	}
}

func (c *Connection) handleDelivery(handler wrappedHandler, delivery amqp.Delivery, deliveryInfo DeliveryInfo) {
	tracingCtx := extractToContext(delivery.Headers)
	span := trace.SpanFromContext(tracingCtx)
	if !span.SpanContext().IsValid() {
		tracingCtx, span = otel.Tracer("amqp").Start(context.Background(), fmt.Sprintf("%s#%s", deliveryInfo.Queue, delivery.RoutingKey))
	}
	defer span.End()
	handlerCtx := injectRoutingKeyToTypeContext(tracingCtx, c.keyToType)
	startTime := time.Now()

	uevt := unmarshalEvent{DeliveryInfo: deliveryInfo, Payload: delivery.Body}
	if err := handler(handlerCtx, uevt); err != nil {
		elapsed := time.Since(startTime).Milliseconds()
		notifyEventHandlerFailed(c.notificationCh, deliveryInfo.RoutingKey, elapsed, err)
		if errors.Is(err, ErrParseJSON) {
			eventNotParsable(deliveryInfo.Queue, deliveryInfo.RoutingKey)
			_ = delivery.Nack(false, false)
		} else if errors.Is(err, ErrNoMessageTypeForRouteKey) {
			eventWithoutHandler(deliveryInfo.Queue, deliveryInfo.RoutingKey)
			_ = delivery.Reject(false)
		} else {
			eventNack(deliveryInfo.Queue, deliveryInfo.RoutingKey, elapsed)
			_ = delivery.Nack(false, true)
		}
		return
	}

	elapsed := time.Since(startTime).Milliseconds()
	notifyEventHandlerSucceed(c.notificationCh, deliveryInfo.RoutingKey, elapsed)
	_ = delivery.Ack(false)
	eventAck(deliveryInfo.Queue, deliveryInfo.RoutingKey, elapsed)
}

func (c *Connection) messageHandlerBindQueueToExchange(cfg *QueueBindingConfig) error {
	if err := c.addHandler(cfg.queueName, cfg.routingKey, cfg.handler); err != nil {
		return err
	}

	if err := exchangeDeclare(c.channel, cfg.exchangeName, cfg.kind); err != nil {
		return err
	}
	if err := queueDeclare(c.channel, cfg.queueName, cfg.transient); err != nil {
		return err
	}
	return c.channel.QueueBind(cfg.queueName, cfg.routingKey, cfg.exchangeName, false, cfg.headers)
}

func exchangeDeclare(channel AmqpChannel, name string, kind kind) error {
	return channel.ExchangeDeclare(name, string(kind), true, false, false, false, nil)
}

func queueDeclare(channel AmqpChannel, name string, transient bool) error {
	_, err := channel.QueueDeclare(name, !transient, transient, false, false, queueDeclareExpiration)
	return err
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
		queueHandlers: &queueHandlers{},
		keyToType:     make(map[string]reflect.Type),
		typeToKey:     make(map[reflect.Type]string),
	}
}

func (c *Connection) setup() error {
	for _, queue := range c.queueHandlers.queues() {
		consumer, err := consume(c.channel, queue.Name)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", queue.Name, err)
		}
		go c.divertToMessageHandlers(consumer, queue)
	}
	return nil
}

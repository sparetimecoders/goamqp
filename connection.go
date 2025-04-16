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
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is a wrapper around the actual amqp.Connection and amqp.Channel
type Connection struct {
	started        bool
	serviceName    string
	amqpUri        amqp.URI
	connection     amqpConnection
	channel        AmqpChannel
	queueConsumers *queueConsumers
	typeToKey      typeToRoutingKey
	keyToType      routingKeyToType
	notificationCh chan<- Notification
	errorCh        chan<- ErrorNotification
	spanNameFn     func(DeliveryInfo) string
}

// ServiceResponsePublisher represents the function that is called to publish a response
type ServiceResponsePublisher[T any] func(ctx context.Context, targetService, routingKey string, event T) error

var (
	// ErrEmptySuffix returned when an empty suffix is passed
	ErrEmptySuffix = fmt.Errorf("empty queue suffix not allowed")
	// ErrAlreadyStarted returned when Start is called multiple times
	ErrAlreadyStarted = fmt.Errorf("already started")
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
	if err := c.channel.Qos(20, 0, false); err != nil {
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

func (c *Connection) messageHandlerBindQueueToExchange(cfg *ConsumerConfig) error {
	if err := c.queueConsumers.add(cfg.queueName, cfg.routingKey, cfg.handler); err != nil {
		return err
	}

	if err := exchangeDeclare(c.channel, cfg.exchangeName, cfg.kind); err != nil {
		return err
	}
	if err := queueDeclare(c.channel, cfg); err != nil {
		return err
	}
	return c.channel.QueueBind(cfg.queueName, cfg.routingKey, cfg.exchangeName, false, cfg.queueBindingHeaders)
}

func exchangeDeclare(channel AmqpChannel, name string, kind string) error {
	return channel.ExchangeDeclare(name, string(kind), true, false, false, false, nil)
}

func queueDeclare(channel AmqpChannel, cfg *ConsumerConfig) error {
	_, err := channel.QueueDeclare(cfg.queueName, true, false, false, false, cfg.queueHeaders)
	return err
}

const (
	headerService = "service"
)

const contentType = "application/json"

var (
	deleteQueueAfter    = 5 * 24 * time.Hour
	defaultQueueOptions = amqp.Table{
		amqp.QueueTypeArg:            amqp.QueueTypeQuorum,
		amqp.SingleActiveConsumerArg: true,
		amqp.QueueTTLArg:             int(deleteQueueAfter.Seconds() * 1000)}
)

func newConnection(serviceName string, uri amqp.URI) *Connection {
	return &Connection{
		serviceName: serviceName,
		amqpUri:     uri,
		queueConsumers: &queueConsumers{
			consumers:  make(map[string]*queueConsumer),
			spanNameFn: spanNameFn,
		},
		keyToType: make(map[string]reflect.Type),
		typeToKey: make(map[reflect.Type]string),
	}
}

func (c *Connection) setup() error {
	for _, consumer := range (*c).queueConsumers.consumers {
		if deliveries, err := consumer.consume(c.channel, c.keyToType, c.notificationCh, c.errorCh); err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", consumer.queue, err)
		} else {
			go consumer.loop(deliveries)
		}
	}
	return nil
}

type routingKeyToType map[string]reflect.Type

type typeToRoutingKey map[reflect.Type]string

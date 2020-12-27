package goamqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"
)

// DelayedMessage indicates that the message will not be delivered before the given TTL has passed.
//
// The delayed messaging plugin must be installed on the RabbitMQ server to enable this functionality.
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
type DelayedMessage interface {
	// The delay time in milliseconds. Please note that the maximum delay time is 2^32-1 milliseconds
	TTL() time.Duration
}

// DelayMaxMillis is the longest delay allowed by the RabbitMQ Delayed Message plugin
const DelayMaxMillis = math.MaxUint32

// NewFromURL creates a new Connection from an URL
func NewFromURL(serviceName string, amqpURL string) (*Connection, error) {
	amqpConfig, err := ParseAmqpURL(amqpURL)
	if err != nil {
		return nil, err
	}
	return newConnection(serviceName, amqpConfig), nil
}

// New creates a new Connection from config
func New(serviceName string, config AmqpConfig) *Connection {
	return newConnection(serviceName, config)
}

// Setup is a setup function that takes a Connection and use it to setup AMQP
// An example is to create exchanges and queues
type Setup func(conn *Connection) error

// CloseListener receives a callback when the AMQP Channel gets closed
func CloseListener(e chan error) Setup {
	return func(c *Connection) error {
		temp := make(chan *amqp.Error)
		go func() {
			for {
				ev := <-temp
				e <- errors.New(ev.Error())
			}
		}()
		c.channel.NotifyClose(temp)
		return nil
	}
}

// UseMessageLogger allows a MessageLogger to be used when log in/outgoing messages
func UseMessageLogger(logger MessageLogger) Setup {
	return func(c *Connection) error {
		if logger == nil {
			return errors.New("cannot use nil as MessageLogger")
		}
		c.messageLogger = logger
		return nil
	}
}

// WithDelayedMessaging configures exchanges to accept delayed messages
func WithDelayedMessaging() Setup {
	return func(conn *Connection) error {
		conn.config.DelayedMessage = true
		return nil
	}
}

// WithPrefetchLimit configures the number of messages to prefetch from the server.
func WithPrefetchLimit(limit int) Setup {
	return func(conn *Connection) error {
		return conn.channel.Qos(limit, 0, true)
	}
}

// TransientEventStreamListener sets up ap a event stream listener that will get removed when the channel is closed
func TransientEventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Setup {
	return func(c *Connection) error {
		queueName := serviceEventRandomQueueName(c.serviceName)
		exchangeName := eventsExchangeName()
		if err := c.addMsgHandler(queueName, routingKey, handler, eventType); err != nil {
			return err
		}

		if err := c.exchangeDeclare(c.channel, exchangeName, "topic"); err != nil {
			return err
		}
		if err := transientQueueDeclare(c.channel, queueName); err != nil {
			return err
		}
		return c.channel.QueueBind(queueName, routingKey, exchangeName, false, amqp.Table{})
	}
}

// EventStreamListener sets up ap a durable, persistent event stream listener
func EventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Setup {
	return func(c *Connection) error {
		queueName := serviceEventQueueName(c.serviceName)
		exchangeName := eventsExchangeName()

		return c.messageHandlerBindQueueToExchange(queueName, exchangeName, routingKey, "topic", handler, eventType, amqp.Table{})
	}
}

// EventStreamPublisher sets up ap a event stream publisher
func EventStreamPublisher(publisher *Publisher) Setup {
	return func(c *Connection) error {
		if err := c.exchangeDeclare(c.channel, eventsExchangeName(), "topic"); err != nil {
			return err
		}
		publisher.connection = c
		publisher.exchange = eventsExchangeName()
		return nil
	}
}

// ServicePublisher sets up ap a service publisher
func ServicePublisher(targetService string, publisher *Publisher) Setup {
	return func(c *Connection) error {
		reqExchangeName := serviceRequestExchangeName(targetService)
		publisher.connection = c
		publisher.exchange = reqExchangeName
		if err := c.exchangeDeclare(c.channel, reqExchangeName, "direct"); err != nil {
			return err
		}
		return nil
	}
}

// RequestResponseHandler sets up ap a service stream listener
func RequestResponseHandler(routingKey string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) Setup {
	return func(c *Connection) error {
		reqExchangeName := serviceRequestExchangeName(c.serviceName)
		reqQueueName := serviceRequestQueueName(c.serviceName)

		resExchangeName := serviceResponseExchangeName(c.serviceName)
		if err := c.addResponseHandler(reqQueueName, routingKey, resExchangeName, handler, eventType); err != nil {
			return err
		}

		if err := c.exchangeDeclare(c.channel, resExchangeName, "headers"); err != nil {
			return err
		}
		if err := c.exchangeDeclare(c.channel, reqExchangeName, "direct"); err != nil {
			return err
		}
		if err := queueDeclare(c.channel, reqQueueName); err != nil {
			return err
		}
		return c.channel.QueueBind(reqQueueName, routingKey, reqExchangeName, false, amqp.Table{})
	}
}

// NewPublisher returns a publisher that can be used to send messages
func NewPublisher(routes ...Route) *Publisher {
	r := make(map[reflect.Type]string)
	for _, route := range routes {
		r[reflect.TypeOf(route.Type)] = route.Key
	}

	return &Publisher{
		typeToRoutingKey: r,
	}
}

// Route defines the routekey to be used for a message type
type Route struct {
	Type interface{}
	Key  string
}

// Publisher is used to send messages
type Publisher struct {
	connection       *Connection
	exchange         string
	typeToRoutingKey routes
}

// Publish publishes a message to a given topic
func (p *Publisher) Publish(msg interface{}) error {
	headers := amqp.Table{}
	headers["service"] = p.connection.serviceName
	t := reflect.TypeOf(msg)
	key := t
	if t.Kind() == reflect.Ptr {
		key = t.Elem()
	}
	if key, ok := p.typeToRoutingKey[key]; ok {
		return p.connection.publishMessage(msg, key, p.exchange, headers)
	}
	return fmt.Errorf("no routingkey configured for message of type %s", t)
}

// PublishNotify see amqp.Channel.Confirm
func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.channel.NotifyPublish(confirm)
		return c.channel.Confirm(false)
	}
}

// Start setups the amqp queues and exchanges defined by opts
func (c *Connection) Start(opts ...Setup) error {
	if c.started {
		return fmt.Errorf("already started")
	}
	c.messageLogger = NopLogger()

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
		if getFuncName(f) == "gitlab.com/sparetimecoders/goamqp.WithDelayedMessaging.func1" {
			_ = f(c)
		}
	}

	for _, f := range opts {
		if err := f(c); err != nil {
			return fmt.Errorf("setup function <%s> failed, %v", getFuncName(f), err)
		}
	}

	if err := c.setup(); err != nil {
		return err
	}

	c.started = true
	return nil
}

func getFuncName(f Setup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// Close closes the amqp connection, see amqp.Connection.Close
func (c *Connection) Close() error {
	return c.connection.Close()
}

func newConnection(serviceName string, config AmqpConfig) *Connection {
	return &Connection{
		serviceName: serviceName,
		config:      config,
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

type routes map[reflect.Type]string

type queueRoutingKey struct {
	Queue      string
	RoutingKey string
}

type messageHandlerInvoker struct {
	msgHandler       func(interface{}) bool
	ResponseHandler  func(interface{}) (interface{}, bool)
	ResponseExchange string
	queueRoutingKey
	EventType reflect.Type
}

// Connection is a wrapper around the actual amqp.Connection and amqp.Channel
type Connection struct {
	started       bool
	serviceName   string
	config        AmqpConfig
	connection    amqpConnection
	channel       AmqpChannel
	handlers      map[queueRoutingKey]messageHandlerInvoker
	messageLogger MessageLogger
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
	Qos(prefetchCount, prefetchSize int, global bool) error
}

var _ AmqpChannel = &amqp.Channel{}

type amqpConnection interface {
	io.Closer
	Channel() (*amqp.Channel, error)
}

var deleteQueueAfter = 5 * 24 * time.Hour

func dialConfig(url string, cfg amqp.Config) (amqpConnection, error) {
	return amqp.DialConfig(url, cfg)
}

var dialAmqp = dialConfig

func amqpVersion() string {
	// NOTE: this doesn't work outside of a build, se we can't really test it
	if x, ok := debug.ReadBuildInfo(); ok {
		for _, y := range x.Deps {
			if y.Path == "gitlab.com/sparetimecoders/goamqp" {
				return y.Version
			}
		}
	}
	return "_unknown_"
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
		amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)},
	)
	return err
}

func transientQueueDeclare(channel AmqpChannel, name string) error {
	_, err := channel.QueueDeclare(name,
		false,
		true,
		false,
		false,
		amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)},
	)
	return err
}

func amqpConfig(serviceName string) amqp.Config {
	return amqp.Config{
		Properties: amqp.Table{
			"connection_name": fmt.Sprintf("%s#%+v#@%s", serviceName, amqpVersion(), hostName()),
		},
	}
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

	conn, err := dialAmqp(c.config.AmqpURL(), cfg)
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

func (c *Connection) exchangeDeclare(channel AmqpChannel, name, kind string) error {
	args := amqp.Table{}
	if c.config.DelayedMessage {
		args["x-delayed-type"] = kind
		kind = "x-delayed-message"
	}

	return channel.ExchangeDeclare(
		name,
		kind,
		true,
		false,
		false,
		false,
		args,
	)
}

func (c *Connection) addMsgHandler(queueName, routingKey string, handler func(interface{}) bool, eventType reflect.Type) error {
	return c.addHandler(queueName, routingKey, eventType, func() messageHandlerInvoker {
		return messageHandlerInvoker{msgHandler: handler, queueRoutingKey: queueRoutingKey{Queue: queueName, RoutingKey: routingKey}, EventType: eventType}
	})
}

func (c *Connection) addResponseHandler(queueName, routingKey, serviceResponseExchangeName string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) error {
	return c.addHandler(queueName, routingKey, eventType, func() messageHandlerInvoker {
		return messageHandlerInvoker{ResponseHandler: handler, queueRoutingKey: queueRoutingKey{Queue: queueName, RoutingKey: routingKey}, ResponseExchange: serviceResponseExchangeName, EventType: eventType}
	})
}

func (c *Connection) addHandler(queueName, routingKey string, eventType reflect.Type, mHI func() messageHandlerInvoker) error {
	uniqueKey := queueRoutingKey{Queue: queueName, RoutingKey: routingKey}

	if existing, exist := c.handlers[uniqueKey]; exist {
		return fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s", routingKey, queueName, existing.EventType, eventType)
	}
	c.handlers[uniqueKey] = mHI()

	return nil
}

func (c *Connection) handleMessage(d amqp.Delivery, handler func(interface{}) bool, eventType reflect.Type, routingKey string) {
	message, err := c.parseMessage(d.Body, eventType, routingKey)
	if err != nil {
		_ = d.Reject(false)
	} else {
		if success := handler(message); success {
			_ = d.Ack(false)
		} else {
			_ = d.Nack(false, true)
		}
	}
}

func (c *Connection) handleRequestResponse(d amqp.Delivery, invoker messageHandlerInvoker, routingKey string) {
	handler := invoker.ResponseHandler
	message, err := c.parseMessage(d.Body, invoker.EventType, routingKey)
	if err != nil {
		_ = d.Reject(false)
	} else {
		if response, success := handler(message); success {
			headers := amqp.Table{}
			headers["service"] = d.Headers["service"]
			if err := c.publishMessage(response, invoker.RoutingKey, invoker.ResponseExchange, headers); err != nil {
				_ = d.Nack(false, false)
			} else {
				_ = d.Ack(false)
			}
		} else {
			_ = d.Nack(false, true)
		}
	}
}

func (c *Connection) parseMessage(jsonContent []byte, eventType reflect.Type, routingKey string) (interface{}, error) {
	c.messageLogger(jsonContent, eventType, routingKey, false)
	target := reflect.New(eventType).Interface()
	if err := json.Unmarshal(jsonContent, &target); err != nil {
		return nil, err
	}

	return target, nil
}

func (c *Connection) publishMessage(msg interface{}, routingKey, exchangeName string, headers amqp.Table) error {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.messageLogger(jsonBytes, reflect.TypeOf(msg), routingKey, true)

	if dm, ok := msg.(DelayedMessage); ok {
		delayMillis := dm.TTL().Seconds() * 1000
		if delayMillis > DelayMaxMillis {
			fmt.Printf("delay time value for type %s is too large, please check your code", reflect.TypeOf(msg))
		}
		headers["x-delay"] = fmt.Sprintf("%.0f", delayMillis)
	}

	publishing := amqp.Publishing{
		Body:         jsonBytes,
		ContentType:  "application/json",
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
			if h.ResponseExchange != "" {
				c.handleRequestResponse(d, h, d.RoutingKey)
			} else {
				c.handleMessage(d, h.msgHandler, h.EventType, d.RoutingKey)
			}
		} else {
			// Unhandled message
			_ = d.Reject(false)
		}
	}
}

func (c *Connection) messageHandlerBindQueueToExchange(queueName, exchangeName, routingKey, kind string, handler func(interface{}) bool, eventType reflect.Type, headers amqp.Table) error {
	if err := c.addMsgHandler(queueName, routingKey, handler, eventType); err != nil {
		return err
	}

	if err := c.exchangeDeclare(c.channel, exchangeName, kind); err != nil {
		return err
	}
	if err := queueDeclare(c.channel, queueName); err != nil {
		return err
	}
	return c.channel.QueueBind(queueName, routingKey, exchangeName, false, headers)
}

package goamqp

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// EventType is the type of the message being sent
type EventType reflect.Type

// Setup is a setup function that takes a Connection and use it to setup AMQP
// An example is to create exchanges and queues
type Setup func(conn *Connection) error

//HandlerFunc is used to process an incoming message
// If processing fails, an error should be returned
// The optional handlerResp is used automatically when setting up a RequestResponseHandler, otherwise ignored
type HandlerFunc func(msg interface{}, headers Headers) (response interface{}, err error)

// Route defines the routing key to be used for a message type
type Route struct {
	Type interface{}
	Key  string
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
	log           Logger
}

// ServiceResponsePublisher represents the function that is called to publish a response
type ServiceResponsePublisher func(targetService, routingKey string, msg interface{}) error

// QueueBindingConfig is a wrapper around the actual amqp queue configuration
type QueueBindingConfig struct {
	routingKey   string
	handler      HandlerFunc
	eventType    EventType
	queueName    string
	exchangeName string
	kind         kind
	headers      amqp.Table
}

// QueueBindingConfigSetup is a setup function that takes a QueueBindingConfig and provide custom changes to the
// configuration
type QueueBindingConfigSetup func(config *QueueBindingConfig) error

// AddQueueNameSuffix appends the provided suffix to the queue name
// Useful when multiple listeners are needed for a routing key in the same service
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
)

// SendingService returns the name of the service that produced the message
// Can be used to send a handlerResponse, see PublishServiceResponse
func SendingService(headers Headers) (string, error) {
	if h, exist := headers[headerService]; exist {
		switch v := h.(type) {
		case string:
			return v, nil
		}
	}
	return "", errors.New("no service found")
}

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

// UseLogger allows a Logger to be used to log errors during processing of messages
func UseLogger(logger Logger) Setup {
	return func(c *Connection) error {
		if logger == nil {
			return errors.New("cannot use nil as Logger")
		}
		c.log = logger
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

// WithPrefetchLimit configures the number of messages to prefetch from the server.
func WithPrefetchLimit(limit int) Setup {
	return func(conn *Connection) error {
		return conn.channel.Qos(limit, 0, true)
	}
}

// TransientEventStreamListener sets up ap a event stream listener that will get removed when the connection is closed
// TODO Document how messages flow, reference docs.md?
func TransientEventStreamListener(routingKey string, handler HandlerFunc, eventType EventType) Setup {
	return func(c *Connection) error {
		queueName := serviceEventRandomQueueName(c.serviceName)
		exchangeName := eventsExchangeName()
		if err := c.addHandler(queueName, routingKey, eventType, messageHandlerInvoker{
			msgHandler: handler,
			queueRoutingKey: queueRoutingKey{
				Queue:      queueName,
				RoutingKey: routingKey,
			},
			eventType: eventType,
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

// EventStreamListener sets up ap a durable, persistent event stream listener
// TODO Document how messages flow, reference docs.md?
func EventStreamListener(routingKey string, handler HandlerFunc, eventType EventType, opts ...QueueBindingConfigSetup) Setup {
	return func(c *Connection) error {
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventType,
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

// EventStreamPublisher sets up ap a event stream publisher
// TODO Document how messages flow, reference docs.md?
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
			Header{Key: "CC", Value: []interface{}{destinationQueueName}},
		); err != nil {
			return err
		}
		publisher.exchange = ""
		return nil
	}
}

// ServiceResponseListener is a specialization of EventStreamListener
// It sets up ap a durable, persistent listener (exchange->queue) for responses from targetService
// TODO Document how messages flow, reference docs.md?
func ServiceResponseListener(targetService, routingKey string, handler HandlerFunc, eventType EventType) Setup {
	return func(c *Connection) error {
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventType,
			queueName:    serviceResponseExchangeName(c.serviceName),
			exchangeName: serviceResponseExchangeName(targetService),
			kind:         kindHeaders,
			headers:      amqp.Table{headerService: c.serviceName},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// ServiceRequestListener is a specialization of EventStreamListener
// It sets up ap a durable, persistent listener (exchange->queue) for message to the service owning the Connection
// TODO Document how messages flow, reference docs.md?
func ServiceRequestListener(routingKey string, handler HandlerFunc, eventType EventType) Setup {
	return func(c *Connection) error {

		resExchangeName := serviceResponseExchangeName(c.serviceName)
		if err := c.exchangeDeclare(c.channel, resExchangeName, kindHeaders); err != nil {
			return errors.Wrapf(err, "failed to create exchange %s", resExchangeName)
		}

		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      handler,
			eventType:    eventType,
			queueName:    serviceRequestQueueName(c.serviceName),
			exchangeName: serviceRequestExchangeName(c.serviceName),
			kind:         kindDirect,
			headers:      amqp.Table{},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// ServicePublisher sets up ap a publisher, that sends messages to the targetService
// TODO Document how messages flow, reference docs.md?
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

// RequestResponseHandler is a convenience func to setup ServiceRequestListener and combines it with PublishServiceResponse
// TODO Document how messages flow, reference docs.md?
func RequestResponseHandler(routingKey string, handler HandlerFunc, eventType EventType) Setup {
	return func(c *Connection) error {
		responseHandlerWrapper := ResponseWrapper(handler, routingKey, c.PublishServiceResponse)
		return ServiceRequestListener(routingKey, responseHandlerWrapper, eventType)(c)
	}
}

// ResponseWrapper is...TODO make this internal?
func ResponseWrapper(handler HandlerFunc, routingKey string, publisher ServiceResponsePublisher) HandlerFunc {
	return func(msg interface{}, headers Headers) (response interface{}, err error) {
		resp, err := handler(msg, headers)
		if err != nil {
			return nil, errors.Wrap(err, "failed to process message")
		}
		if resp != nil {
			service, err := SendingService(headers)
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

// PublishNotify see amqp.Channel.Confirm
func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.channel.NotifyPublish(confirm)
		return c.channel.Confirm(false)
	}
}

// PublishServiceResponse sends a message to targetService as a handlerResp
// TODO Document how messages flow, reference docs.md?
func (c *Connection) PublishServiceResponse(targetService, routingKey string, msg interface{}) error {
	return c.publishMessage(msg, routingKey, serviceResponseExchangeName(c.serviceName), amqp.Table{headerService: targetService})
}

// Start setups the amqp queues and exchanges defined by opts
func (c *Connection) Start(opts ...Setup) error {
	if c.started {
		return fmt.Errorf("already started")
	}
	c.messageLogger = NoOpMessageLogger()
	c.log = &noOpLogger{}
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

func (c *Connection) addHandler(queueName, routingKey string, eventType EventType, mHI messageHandlerInvoker) error {
	uniqueKey := queueRoutingKey{Queue: queueName, RoutingKey: routingKey}

	if existing, exist := c.handlers[uniqueKey]; exist {
		return fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s, consider using AddQueueNameSuffix", routingKey, queueName, existing.eventType, eventType)
	}
	c.handlers[uniqueKey] = mHI

	return nil
}

func (c *Connection) handleMessage(d amqp.Delivery, handler HandlerFunc, eventType EventType, routingKey string) {
	message, err := c.parseMessage(d.Body, eventType, routingKey)
	if err != nil {
		_ = d.Reject(false)
	} else {
		if _, err := handler(message, headers(d.Headers)); err == nil {
			_ = d.Ack(false)
		} else {
			c.log.Errorf("failed to process message %s", err)
			_ = d.Nack(false, true)
		}
	}
}

func (c *Connection) parseMessage(jsonContent []byte, eventType EventType, routingKey string) (interface{}, error) {
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
			// Unhandled message
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
	msgHandler HandlerFunc
	queueRoutingKey
	eventType EventType
}

func getSetupFuncName(f Setup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

func getQueueBindingConfigSetupFuncName(f QueueBindingConfigSetup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

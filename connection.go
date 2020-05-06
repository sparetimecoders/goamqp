package goamqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io"
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
	TTL() time.Duration
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

type Setup func(conn *Connection) error

func CloseListener(e chan error) Setup {
	return func(c *Connection) error {
		temp := make(chan *amqp.Error)
		go func() {
			for {
				ev := <-temp
				e <- errors.New(ev.Error())
			}
		}()
		c.Channel.NotifyClose(temp)
		return nil
	}
}

func TransientEventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Setup {
	return func(c *Connection) error {
		queueName := serviceEventRandomQueueName(c.serviceName)
		exchangeName := eventsExchangeName()
		if err := c.addMsgHandler(queueName, routingKey, handler, eventType); err != nil {
			return err
		}

		if err := c.exchangeDeclare(c.Channel, exchangeName, "topic"); err != nil {
			return err
		}
		if err := transientQueueDeclare(c.Channel, queueName); err != nil {
			return err
		}
		return c.Channel.QueueBind(queueName, routingKey, exchangeName, false, amqp.Table{})
	}
}

func EventStreamPublisher(routingKey string, publisher chan interface{}) Setup {
	return func(c *Connection) error {
		if err := c.exchangeDeclare(c.Channel, eventsExchangeName(), "topic"); err != nil {
			return err
		}
		go c.publish(publisher, routingKey, eventsExchangeName())
		return nil
	}
}

func EventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Setup {
	return func(c *Connection) error {
		queueName := serviceEventQueueName(c.serviceName)
		exchangeName := eventsExchangeName()

		return c.messageHandlerBindQueueToExchange(queueName, exchangeName, routingKey, "topic", handler, eventType, amqp.Table{})
	}
}

func ServicePublisher(targetService, routingKey string, publisher chan interface{}, handler func(interface{}) bool, eventType reflect.Type) Setup {
	return func(c *Connection) error {
		if handler != nil {
			resQueueName := serviceResponseQueueName(targetService, c.serviceName)
			resExchangeName := serviceResponseExchangeName(targetService)

			if err := c.messageHandlerBindQueueToExchange(resQueueName, resExchangeName, routingKey, "headers", handler, eventType, amqp.Table{"x-match": "all"}); err != nil {
				return err
			}
		}

		reqExchangeName := serviceRequestExchangeName(targetService)

		if err := c.exchangeDeclare(c.Channel, reqExchangeName, "direct"); err != nil {
			return err
		}
		go c.publish(publisher, routingKey, reqExchangeName)

		return nil
	}
}

func RequestResponseHandler(routingKey string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) Setup {
	return func(c *Connection) error {
		reqExchangeName := serviceRequestExchangeName(c.serviceName)
		reqQueueName := serviceRequestQueueName(c.serviceName)

		resExchangeName := serviceResponseExchangeName(c.serviceName)
		if err := c.addResponseHandler(reqQueueName, routingKey, resExchangeName, handler, eventType); err != nil {
			return err
		}

		if err := c.exchangeDeclare(c.Channel, resExchangeName, "headers"); err != nil {
			return err
		}
		if err := c.exchangeDeclare(c.Channel, reqExchangeName, "direct"); err != nil {
			return err
		}
		if err := queueDeclare(c.Channel, reqQueueName); err != nil {
			return err
		}
		return c.Channel.QueueBind(reqQueueName, routingKey, reqExchangeName, false, amqp.Table{})
	}
}

func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.Channel.NotifyPublish(confirm)
		return c.Channel.Confirm(false)
	}
}

func (c *Connection) Start(opts ...Setup) error {
	if c.started {
		return fmt.Errorf("already started")
	}

	if c.Channel == nil {
		err := c.connectToAmqpURL()
		if err != nil {
			return err
		}
	}

	for _, f := range opts {
		if err := f(c); err != nil {
			return fmt.Errorf("setup function <%s> failed, %v", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), err)
		}
	}

	if err := c.setup(); err != nil {
		return err
	}

	c.started = true
	return nil
}

func (c *Connection) Close() error {
	return c.connection.Close()
}

func newConnection(serviceName string, config AmqpConfig) *Connection {
	return &Connection{
		serviceName: serviceName,
		config:      config,
		Handlers:    make(map[QueueRoutingKey]MessageHandlerInvoker),
	}
}

func (c *Connection) setup() error {
	queues := make(map[string][]MessageHandlerInvoker)

	for qr, h := range c.Handlers {
		queues[qr.Queue] = append(queues[qr.Queue], h)
	}

	for q, h := range queues {
		queueHandlers := make(map[string]MessageHandlerInvoker)
		for _, kh := range h {
			queueHandlers[kh.RoutingKey] = kh
		}
		consumer, err := consume(c.Channel, q)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", q, err)
		}
		go divertToMessageHandlers(c.Channel, consumer, queueHandlers)
	}
	return nil
}

type QueueRoutingKey struct {
	Queue      string
	RoutingKey string
}

type MessageHandlerInvoker struct {
	msgHandler       func(interface{}) bool
	ResponseHandler  func(interface{}) (interface{}, bool)
	ResponseExchange string
	QueueRoutingKey
	EventType reflect.Type
}

type Connection struct {
	started     bool
	serviceName string
	config      AmqpConfig
	connection  amqpConnection
	Channel     AmqpChannel
	Handlers    map[QueueRoutingKey]MessageHandlerInvoker
}

type AmqpChannel interface {
	QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Confirm(noWait bool) error
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
	if hostname, err := os.Hostname(); err != nil {
		return "_unknown_"
	} else {
		return hostname
	}
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

	c.Channel = ch
	c.connection = conn
	return nil
}

func (c *Connection) publish(p <-chan interface{}, routingKey, exchangeName string) {
	headers := amqp.Table{}
	headers["service"] = c.serviceName
	for msg := range p {
		err := publishMessage(c.Channel, msg, routingKey, exchangeName, headers)
		if err != nil {
			fmt.Printf("failed to publish message %+v", msg)
		}
	}
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
	return c.addHandler(queueName, routingKey, eventType, func() MessageHandlerInvoker {
		return MessageHandlerInvoker{msgHandler: handler, QueueRoutingKey: QueueRoutingKey{Queue: queueName, RoutingKey: routingKey}, EventType: eventType}
	})
}

func (c *Connection) addResponseHandler(queueName, routingKey, serviceResponseExchangeName string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) error {
	return c.addHandler(queueName, routingKey, eventType, func() MessageHandlerInvoker {
		return MessageHandlerInvoker{ResponseHandler: handler, QueueRoutingKey: QueueRoutingKey{Queue: queueName, RoutingKey: routingKey}, ResponseExchange: serviceResponseExchangeName, EventType: eventType}
	})
}

func (c *Connection) addHandler(queueName, routingKey string, eventType reflect.Type, mHI func() MessageHandlerInvoker) error {
	uniqueKey := QueueRoutingKey{Queue: queueName, RoutingKey: routingKey}

	if existing, exist := c.Handlers[uniqueKey]; exist {
		return fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s", routingKey, queueName, existing.EventType, eventType)
	}
	c.Handlers[uniqueKey] = mHI()

	return nil
}

func handleMessage(d amqp.Delivery, handler func(interface{}) bool, eventType reflect.Type) {
	message, err := parseMessage(d.Body, eventType)
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

func handleRequestResponse(channel AmqpChannel, d amqp.Delivery, invoker MessageHandlerInvoker) {
	handler := invoker.ResponseHandler
	message, err := parseMessage(d.Body, invoker.EventType)
	if err != nil {
		_ = d.Reject(false)
	} else {
		if response, success := handler(message); success {
			headers := amqp.Table{}
			headers["service"] = d.Headers["service"]
			if err := publishMessage(channel, response, invoker.RoutingKey, invoker.ResponseExchange, headers); err != nil {
				_ = d.Nack(false, false)
			} else {
				_ = d.Ack(false)
			}
		} else {
			_ = d.Nack(false, true)
		}
	}
}

func parseMessage(jsonContent []byte, eventType reflect.Type) (interface{}, error) {
	target := reflect.New(eventType).Interface()
	if err := json.Unmarshal(jsonContent, &target); err != nil {
		return nil, err
	}

	return target, nil
}

func publishMessage(channel AmqpChannel, msg interface{}, routingKey, exchangeName string, headers amqp.Table) error {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if dm, ok := msg.(DelayedMessage); ok {
		headers["x-delay"] = fmt.Sprintf("%.0f", dm.TTL().Seconds()*1000)
	}

	publishing := amqp.Publishing{
		Body:         jsonBytes,
		ContentType:  "application/json",
		DeliveryMode: 2,
		Headers:      headers,
	}

	return channel.Publish(exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)
}

func divertToMessageHandlers(channel AmqpChannel, deliveries <-chan amqp.Delivery, handlers map[string]MessageHandlerInvoker) {
	for d := range deliveries {
		if h, ok := handlers[d.RoutingKey]; ok {
			if h.ResponseExchange != "" {
				handleRequestResponse(channel, d, h)
			} else {
				handleMessage(d, h.msgHandler, h.EventType)
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

	if err := c.exchangeDeclare(c.Channel, exchangeName, kind); err != nil {
		return err
	}
	if err := queueDeclare(c.Channel, queueName); err != nil {
		return err
	}
	return c.Channel.QueueBind(queueName, routingKey, exchangeName, false, headers)
}

package goamqp

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"reflect"
	"runtime"
	"strings"
	"time"
)

// DelayedMessage indicates that the message will not be delivered before the given TTL has passed.
//
// The delayed messaging plugin must be installed on the RabbitMQ server to enable this functionality.
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
type DelayedMessage interface {
	TTL() time.Duration
}

// Connection is used to setup new listeners and publishers.
type Connection interface {
	AddEventStreamPublisher(routingKey string, publisher chan interface{}) Connection
	AddEventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Connection
	AddServicePublisher(targetService, routingKey string, publisher chan interface{}, handler func(interface{}) bool, eventType reflect.Type) Connection
	AddRequestResponseHandler(routingKey string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) Connection
	WithLogger(logger Logger) Connection
	AddPublishNotify(confirm chan amqp.Confirmation) Connection
	Start() (io.Closer, error)
}

// NewFromURL creates a new Connection from an URL
func NewFromURL(serviceName string, amqpURL string) Connection {
	amqpConfig, err := ParseAmqpURL(amqpURL)
	if err != nil {
		return newConnection(serviceName, amqpConfig, err)
	}
	return newConnection(serviceName, amqpConfig)
}

// New creates a new Connection from config
func New(serviceName string, config AmqpConfig) Connection {
	return newConnection(serviceName, config)
}

func (c *connection) AddEventStreamPublisher(routingKey string, publisher chan interface{}) Connection {
	c.appendSetupFuncs(func(channel amqpChannel) error {
		return c.exchangeDeclare(channel, eventsExchangeName(), "topic")
	})
	c.appendSetupFuncs(func(channel amqpChannel) error {
		go c.publish(publisher, routingKey, eventsExchangeName())
		return nil
	})
	return c
}

func (c *connection) AddEventStreamListener(routingKey string, handler func(interface{}) bool, eventType reflect.Type) Connection {
	queueName := serviceEventQueueName(c.serviceName)
	exchangeName := eventsExchangeName()
	c.addMsgHandler(queueName, routingKey, handler, eventType)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return c.exchangeDeclare(channel, exchangeName, "topic")
		},
		func(channel amqpChannel) error {
			return c.queueDeclare(queueName)
		},
		func(channel amqpChannel) error {
			return c.bindQueueToExchange(exchangeName, queueName, routingKey, amqp.Table{})
		},
	)
	return c
}

func (c *connection) AddServicePublisher(targetService, routingKey string, publisher chan interface{}, handler func(interface{}) bool, eventType reflect.Type) Connection {
	if handler != nil {
		resQueueName := serviceResponseQueueName(targetService, c.serviceName)
		resExchangeName := serviceResponseExchangeName(targetService)
		c.addMsgHandler(resQueueName, routingKey, handler, eventType)
		c.appendSetupFuncs(func(channel amqpChannel) error {
			return c.exchangeDeclare(channel, resExchangeName, "headers")
		},
			func(channel amqpChannel) error {
				return c.queueDeclare(resQueueName)
			},
			func(channel amqpChannel) error {
				headers := amqp.Table{}
				headers["x-match"] = "all"
				return c.bindQueueToExchange(resExchangeName, resQueueName, routingKey, headers)
			},
		)
	} else {
		c.logger.Infof("handler is nil for service %s, will not setup response listener for target service %s", c.serviceName, targetService)
	}

	reqExchangeName := serviceRequestExchangeName(targetService)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return c.exchangeDeclare(channel, reqExchangeName, "direct")
		},
		func(channel amqpChannel) error {
			go c.publish(publisher, routingKey, reqExchangeName)
			return nil
		},
	)

	return c
}

func (c *connection) AddRequestResponseHandler(routingKey string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) Connection {
	reqExchangeName := serviceRequestExchangeName(c.serviceName)
	reqQueueName := serviceRequestQueueName(c.serviceName)

	resExchangeName := serviceResponseExchangeName(c.serviceName)
	c.addResponseHandler(reqQueueName, routingKey, resExchangeName, handler, eventType)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return c.exchangeDeclare(channel, resExchangeName, "headers")
		},
	)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return c.exchangeDeclare(channel, reqExchangeName, "direct")
		},
		func(channel amqpChannel) error {
			return c.queueDeclare(reqQueueName)
		},
		func(channel amqpChannel) error {
			return c.bindQueueToExchange(reqExchangeName, reqQueueName, routingKey, amqp.Table{})
		},
	)
	return c
}

func (c *connection) AddPublishNotify(confirm chan amqp.Confirmation) Connection {
	c.appendSetupFuncs(func(channel amqpChannel) error {
		c.logger.Info("setting up publish confirmations\n")
		channel.NotifyPublish(confirm)
		return channel.Confirm(false)
	})
	return c
}

func (c *connection) Start() (io.Closer, error) {
	if c.started {
		return c.connection, fmt.Errorf("already started")
	}
	if len(c.setupErrors) > 0 {
		return nil, joinErrors(c.setupErrors...)
	}

	if c.channel == nil {

		err := c.connectToAmqpURL()
		if err != nil {
			return nil, err
		}
	}

	if err := c.setup(); err != nil {
		return nil, err
	}

	go c.handleCloseEvent()
	c.channel.NotifyClose(c.channelCloseListener)
	c.connection.NotifyClose(c.connectionCloseListener)

	c.started = true
	return c, nil
}

func (c *connection) Close() error {
	return c.connection.Close()
}

func newConnection(serviceName string, config AmqpConfig, errors ...error) Connection {
	return &connection{
		serviceName:             serviceName,
		config:                  config,
		handlers:                make(map[queueRoutingKey]messageHandlerInvoker),
		setupErrors:             errors,
		connectionCloseListener: make(chan *amqp.Error),
		channelCloseListener:    make(chan *amqp.Error),
		logger:                  StdLogger(),
	}
}

func (c *connection) setup() error {
	if len(c.setupErrors) > 0 {
		return joinErrors(c.setupErrors...)
	}
	c.logger.Info("setting up exchanges, queue, bindings and handlers")
	for _, f := range c.setupFuncs {
		if err := f(c.channel); err != nil {
			return fmt.Errorf("setup function <%s> failed, %v", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), err)
		}
	}

	queues := make(map[string][]messageHandlerInvoker)

	for qr, h := range c.handlers {
		queues[qr.queue] = append(queues[qr.queue], h)
	}

	for q, h := range queues {
		queueHandlers := make(map[string]messageHandlerInvoker)
		for _, kh := range h {
			queueHandlers[kh.routingKey] = kh
			c.logger.Debugf("setting up flow '%s' filtererd by '%s' to handler\n", q, kh.routingKey)
		}
		consumer, err := consume(c.channel, q)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", q, err)
		}
		go c.divertToMessageHandlers(consumer, queueHandlers)
	}
	c.logger.Info("done setting up exchanges, queue, bindings and handlers")
	return nil
}

type amqpChannel interface {
	QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Confirm(noWait bool) error
}

type amqpConnection interface {
	io.Closer
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
}

var deleteQueueAfter = 5 * 24 * time.Hour

// Internal state
type connection struct {
	started                 bool
	serviceName             string
	config                  AmqpConfig
	connection              amqpConnection
	channel                 amqpChannel
	handlers                map[queueRoutingKey]messageHandlerInvoker
	setupFuncs              []setupFunc
	setupErrors             []error
	connectionCloseListener chan *amqp.Error
	channelCloseListener    chan *amqp.Error
	logger                  Logger
}

var _ amqpChannel = &amqp.Channel{}
var _ Connection = &connection{}

func (c *connection) connectToAmqpURL() error {
	c.logger.Infof("connecting to %s", c.config)

	conn, err := amqp.Dial(c.config.AmqpURL())
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

type queueRoutingKey struct {
	queue      string
	routingKey string
}

type messageHandlerInvoker struct {
	msgHandler       func(interface{}) bool
	responseHandler  func(interface{}) (interface{}, bool)
	responseExchange string
	queueRoutingKey
	eventType reflect.Type
}

func (c *connection) divertToMessageHandlers(deliveries <-chan amqp.Delivery, handlers map[string]messageHandlerInvoker) {
	for d := range deliveries {
		if h, ok := handlers[d.RoutingKey]; ok {
			if h.responseExchange != "" {
				c.handleRequestResponse(d, h)
			} else {
				c.handleMessage(d, h.msgHandler, h.eventType)
			}
		} else {
			// Unhandled message
			c.logger.Debug("unhandled message for key %s from exchange %s - dropping it", d.RoutingKey, d.Exchange)
			_ = d.Reject(false)
		}
	}
}

func (c *connection) WithLogger(logger Logger) Connection {
	c.logger = logger
	return c
}

func parseMessage(jsonContent []byte, eventType reflect.Type) (interface{}, error) {
	target := reflect.New(eventType).Interface()
	if err := json.Unmarshal(jsonContent, &target); err != nil {
		return target, err
	}
	return target, nil
}

func (c *connection) publishMessage(msg interface{}, routingKey, exchangeName string, headers amqp.Table) error {
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
	c.logger.Debugf("Publishing %+v", publishing)

	return c.channel.Publish(exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)
}

func (c *connection) publish(p <-chan interface{}, routingKey, exchangeName string) {
	headers := amqp.Table{}
	headers["service"] = c.serviceName
	for msg := range p {
		err := c.publishMessage(msg, routingKey, exchangeName, headers)
		if err != nil {
			c.logger.Errorf("failed to publish %v", err)
		}
	}
}

func (c *connection) handleRequestResponse(d amqp.Delivery, invoker messageHandlerInvoker) {
	handler := invoker.responseHandler
	message, err := parseMessage(d.Body, invoker.eventType)
	if err != nil {
		c.logger.Errorf("failed to handle message - will drop it, %v", err)
		_ = d.Reject(false)
	} else {
		if response, success := handler(message); success {
			headers := amqp.Table{}
			headers["service"] = d.Headers["service"]
			if err := c.publishMessage(response, invoker.routingKey, invoker.responseExchange, headers); err != nil {
				c.logger.Errorf("Failed to publish response - %s", response)
			}
			c.logger.Tracef("message [%s] handled successfully and will be ACKED", d.MessageId)
			_ = d.Ack(false)
		} else {
			c.logger.Tracef("message handler returned false, message [%s] will be NACKED", d.MessageId)
			_ = d.Nack(false, true)
		}
		// TODO Use something other than MessageId (since its empty...)
	}
}

func (c *connection) handleMessage(d amqp.Delivery, handler func(interface{}) bool, eventType reflect.Type) {
	message, err := parseMessage(d.Body, eventType)
	if err != nil {
		c.logger.Error("failed to handle message - will drop it, %v", err)
		_ = d.Reject(false)
	} else {
		// TODO Use something other than MessageId (since its empty...)
		if success := handler(message); success {
			c.logger.Tracef("message [%s] handled successfully and will be ACKED", d.MessageId)
			_ = d.Ack(false)
		} else {
			c.logger.Tracef("message handler returned false, message [%s] will be NACKED", d.MessageId)
			_ = d.Nack(false, true)
		}
	}
}

func consume(channel amqpChannel, queue string) (<-chan amqp.Delivery, error) {
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

func (c *connection) exchangeDeclare(channel amqpChannel, name, kind string) error {
	c.logger.Debugf("creating exchange with name: %s, and kind: %s", name, kind)
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

func (c *connection) queueDeclare(name string) error {
	c.logger.Debugf("creating queue with name: %s", name)

	_, err := c.channel.QueueDeclare(name,
		true,
		false,
		false,
		false,
		amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)},
	)
	return err
}

func (c *connection) bindQueueToExchange(exchangeName, queueName, routingKey string, headers amqp.Table) error {
	c.logger.Debugf("binding queue with name: %s to exchange: %s with routingkey: %s", queueName, exchangeName, routingKey)
	return c.channel.QueueBind(queueName, routingKey, exchangeName, false, headers)
}

func (c *connection) addMsgHandler(queueName, routingKey string, handler func(interface{}) bool, eventType reflect.Type) {
	uniqueKey := queueRoutingKey{queue: queueName, routingKey: routingKey}
	if existing, exist := c.handlers[uniqueKey]; exist {
		c.addError(fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s", routingKey, queueName, existing.eventType, eventType))
		return
	}
	c.logger.Debugf("routingkey %s for queue %s assigned to handler for type %s", routingKey, queueName, eventType)
	c.handlers[uniqueKey] = messageHandlerInvoker{msgHandler: handler, queueRoutingKey: queueRoutingKey{queue: queueName, routingKey: routingKey}, eventType: eventType}
}

func (c *connection) addResponseHandler(queueName, routingKey, serviceResponseExchangeName string, handler func(interface{}) (interface{}, bool), eventType reflect.Type) {
	uniqueKey := queueRoutingKey{queue: queueName, routingKey: routingKey}
	if existing, exist := c.handlers[uniqueKey]; exist {
		c.addError(fmt.Errorf("routingkey %s for queue %s already assigned to handler for type %s, cannot assign %s", routingKey, queueName, existing.eventType, eventType))
		return
	}
	c.logger.Debugf("routingkey %s for queue %s assigned to handler for type %s", routingKey, queueName, eventType)
	c.handlers[uniqueKey] = messageHandlerInvoker{responseHandler: handler, queueRoutingKey: queueRoutingKey{queue: queueName, routingKey: routingKey}, responseExchange: serviceResponseExchangeName, eventType: eventType}
}

type setupFunc func(channel amqpChannel) error

func (c *connection) appendSetupFuncs(funcs ...setupFunc) {
	c.setupFuncs = append(c.setupFuncs, funcs...)
}

func (c *connection) addError(e error) {
	c.setupErrors = append(c.setupErrors, e)
}

func joinErrors(errors ...error) error {
	var errorStrings []string
	for _, e := range errors {
		errorStrings = append(errorStrings, e.Error())
	}
	return fmt.Errorf("errors found during setup,\n\t%s", strings.Join(errorStrings, "\n\t"))
}

func (c *connection) handleCloseEvent() {
	for {
		select {
		case e, ok := <-c.connectionCloseListener:
			if ok {
				c.logger.Info("Connection closed %+v \n", e)
			}
		case e, ok := <-c.channelCloseListener:
			if ok {
				c.logger.Info("Channel closed %+v \n", e)
			}
		}
	}
}

package goamqp

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"log"
	"reflect"
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
	AddEventStreamListener(routingKey string, handler interface{}) Connection
	AddServicePublisher(targetService, routingKey string, publisher chan interface{}, handler interface{}) Connection
	AddRequestResponseHandler(routingKey string, handler interface{}) Connection
	Start() (io.Closer, error)
}

// NewFromURL creates a new Connection from an URL
func NewFromURL(serviceName string, amqpURL string) Connection {
	amqpConfig, err := ParseAmqpURL(amqpURL)
	return newConnection(serviceName, amqpConfig, err)
}

// New creates a new Connection from config
func New(serviceName string, config AmqpConfig) Connection {
	return newConnection(serviceName, config)
}

func (c *connection) AddEventStreamPublisher(routingKey string, publisher chan interface{}) Connection {
	c.appendSetupFuncs(c.publisherSetupFuncs(routingKey, publisher, eventsExchangeName())...)
	return c
}

func (c *connection) AddEventStreamListener(routingKey string, handler interface{}) Connection {
	queueName := serviceEventQueueName(c.serviceName)
	exchangeName := eventsExchangeName()
	c.addHandler(queueName, routingKey, handler)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return exchangeDeclare(channel, exchangeName, "topic")
		},
		func(channel amqpChannel) error {
			return queueDeclare(channel, queueName)
		},
		func(channel amqpChannel) error {
			return bindQueueToExchange(channel, exchangeName, queueName, routingKey, amqp.Table{})
		},
	)
	return c
}

func (c *connection) AddServicePublisher(targetService, routingKey string, publisher chan interface{}, handler interface{}) Connection {
	if handler != nil {
		resQueueName := serviceResponseQueueName(targetService, c.serviceName)
		resExchangeName := serviceResponseExchangeName(targetService)
		c.addHandler(resQueueName, routingKey, handler)
		c.appendSetupFuncs(func(channel amqpChannel) error {
			return exchangeDeclare(channel, resExchangeName, "headers")
		},
			func(channel amqpChannel) error {
				return queueDeclare(channel, resQueueName)
			},
			func(channel amqpChannel) error {
				headers := amqp.Table{}
				headers["x-match"] = "all"
				return bindQueueToExchange(channel, resExchangeName, resQueueName, routingKey, headers)
			},
		)
	} else {
		log.Printf("handler is nil for service %s, will not setup response listener for target service %s", c.serviceName, targetService)
	}

	reqExchangeName := serviceRequestExchangeName(targetService)

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return exchangeDeclare(channel, reqExchangeName, "direct")
		},
		func(channel amqpChannel) error {
			go c.publish(channel, publisher, routingKey, reqExchangeName)
			return nil
		},
	)

	return c
}

func (c *connection) AddRequestResponseHandler(routingKey string, handler interface{}) Connection {
	reqExchangeName := serviceRequestExchangeName(c.serviceName)
	reqQueueName := serviceRequestQueueName(c.serviceName)
	err := checkRequestResponseHandlerReturns(handler)

	if err != nil {
		log.Println("passed handler is not a request response handler, trying to add it as message handler")
		c.addHandler(reqQueueName, routingKey, handler)
	} else {
		resExchangeName := serviceResponseExchangeName(c.serviceName)
		c.tryAddHandler(reqQueueName, routingKey, resExchangeName, handler)

		c.appendSetupFuncs(
			func(channel amqpChannel) error {
				return exchangeDeclare(channel, resExchangeName, "headers")
			},
		)
	}

	c.appendSetupFuncs(
		func(channel amqpChannel) error {
			return exchangeDeclare(channel, reqExchangeName, "direct")
		},
		func(channel amqpChannel) error {
			return queueDeclare(channel, reqQueueName)
		},
		func(channel amqpChannel) error {
			return bindQueueToExchange(channel, reqExchangeName, reqQueueName, routingKey, amqp.Table{})
		},
	)

	return c
}

func (c *connection) Start() (io.Closer, error) {
	if c.started {
		return c.connection, fmt.Errorf("already started")
	}
	if len(c.setupErrors) > 0 {
		return nil, joinErrors(c.setupErrors...)
	}
	var err error
	c.connection, c.channel, err = connectToAmqpURL(c.config)
	if err != nil {
		return nil, err
	}

	if err := c.setup(); err != nil {
		return nil, err
	}
	c.started = true
	return c, nil
}

func (c *connection) Close() error {
	return c.connection.Close()
}

func newConnection(serviceName string, config AmqpConfig, errors ...error) Connection {
	return &connection{
		serviceName: serviceName,
		config:      config,
		handlers:    make(map[queueRoutingKey]messageHandlerInvoker),
		setupErrors: errors,
	}
}

func (c *connection) setup() error {
	if len(c.setupErrors) > 0 {
		return joinErrors(c.setupErrors...)
	}
	log.Println("setting up exchanges, queue, bindings and handlers")
	for _, f := range c.setupFuncs {
		if err := f(c.channel); err != nil {
			// TODO Which function failed and why
			return fmt.Errorf("setup function <name>?? failed %v", err)
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
			log.Printf("setting up flow '%s' filtererd by '%s' to handler '%s'", q, kh.routingKey, reflect.TypeOf(kh.handler).Elem())

		}
		consumer, err := consume(c.channel, q)
		if err != nil {
			return fmt.Errorf("failed to create consumer for queue %s. %v", q, err)
		}
		go divertToMessageHandlers(c.channel, consumer, queueHandlers)
	}
	log.Println("done setting up exchanges, queue, bindings and handlers")
	return nil
}

type amqpChannel interface {
	QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
}

var deleteQueueAfter = 5 * 24 * time.Hour

// Internal state
type connection struct {
	started     bool
	serviceName string
	config      AmqpConfig
	connection  io.Closer
	channel     amqpChannel
	// Setup state
	handlers    map[queueRoutingKey]messageHandlerInvoker
	setupFuncs  []setupFunc
	setupErrors []error
}

var _ amqpChannel = &amqp.Channel{}
var _ Connection = &connection{}

func connectToAmqpURL(config AmqpConfig) (*amqp.Connection, *amqp.Channel, error) {
	log.Printf("connecting to %s", config)

	conn, err := amqp.Dial(config.AmqpURL())
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, nil
}

type queueRoutingKey struct {
	queue      string
	routingKey string
}

type messageHandlerInvoker struct {
	handler          interface{}
	responseExchange string
	queueRoutingKey
}

func (c *connection) publisherSetupFuncs(routingKey string, publisher chan interface{}, exchangeName string) []setupFunc {
	return []setupFunc{func(channel amqpChannel) error {
		return exchangeDeclare(channel, exchangeName, "topic")
	},
		func(channel amqpChannel) error {
			go c.publish(channel, publisher, routingKey, exchangeName)
			return nil
		},
	}
}

func divertToMessageHandlers(channel amqpChannel, deliveries <-chan amqp.Delivery, handlers map[string]messageHandlerInvoker) {
	for d := range deliveries {
		if h, ok := handlers[d.RoutingKey]; ok {
			if h.responseExchange != "" {
				handleRequestResponse(channel, d, h)
			} else {
				handleMessage(d, h.handler)
			}
		} else {
			// Unhandled message
			log.Printf("unhandled message for key %s from exchange %s - dropping it", d.RoutingKey, d.Exchange)
			d.Reject(false)
		}
	}

}

func parseMessage(jsonContent []byte, handler interface{}) (interface{}, error) {
	inputType := inputType(handler)
	target := reflect.New(inputType).Interface()
	if err := json.Unmarshal(jsonContent, &target); err != nil {
		return target, err
	}
	return target, nil
}

func publishMessage(channel amqpChannel, msg interface{}, routingKey, exchangeName string, headers amqp.Table) error {
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
	log.Printf("Publishing %+v", publishing)
	return channel.Publish(exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)

}
func (c *connection) publish(channel amqpChannel, p <-chan interface{}, routingKey, exchangeName string) {
	headers := amqp.Table{}
	headers["service"] = c.serviceName
	for msg := range p {
		err := publishMessage(channel, msg, routingKey, exchangeName, headers)
		if err != nil {
			log.Printf("failed to publish %v", err)
		}
	}
}

// TODO Remove duplication
func handleRequestResponse(channel amqpChannel, d amqp.Delivery, invoker messageHandlerInvoker) {
	handler := invoker.handler
	message, err := parseMessage(d.Body, handler)
	if err != nil {
		log.Printf("failed to handle message - will drop it, %v", err)
		d.Reject(false)
	} else {
		args := []reflect.Value{reflect.ValueOf(handler), reflect.ValueOf(message).Elem()}
		retValue := getProcessFunction(handler).Call(args)
		success := retValue[1].Bool()
		if success {
			headers := amqp.Table{}
			headers["service"] = d.Headers["service"]
			if err := publishMessage(channel, retValue[0], invoker.routingKey, invoker.responseExchange, headers); err != nil {
				log.Println("Failed to publish response!!!!")
			}
			log.Printf("message [%s] handled successfully and will be ACKED", d.MessageId)
			_ = d.Ack(false)
		} else {
			log.Printf("message handler returned false, message [%s] will be NACKED", d.MessageId)
			_ = d.Nack(false, true)
		}
		// TODO Use something other than MessageId (since its empty...)
	}

}

func handleMessage(d amqp.Delivery, handler interface{}) {
	message, err := parseMessage(d.Body, handler)
	if err != nil {
		log.Printf("failed to handle message - will drop it, %v", err)
		d.Reject(false)
	} else {
		args := []reflect.Value{reflect.ValueOf(handler), reflect.ValueOf(message).Elem()}
		retValue := getProcessFunction(handler).Call(args)

		success := retValue[0].Bool()

		// TODO Use something other than MessageId (since its empty...)
		if success {
			log.Printf("message [%s] handled successfully and will be ACKED", d.MessageId)
			_ = d.Ack(false)
		} else {
			log.Printf("message handler returned false, message [%s] will be NACKED", d.MessageId)
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

func exchangeDeclare(channel amqpChannel, name, kind string) error {
	log.Printf("creating exchange with name: %s, and kind: %s", name, kind)
	args := amqp.Table{}
	args["x-delayed-type"] = kind
	kind = "x-delayed-message"

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

func queueDeclare(channel amqpChannel, name string) error {
	log.Printf("creating queue with name: %s", name)

	_, err := channel.QueueDeclare(name,
		true,
		false,
		false,
		false,
		amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)},
	)
	return err
}

func bindQueueToExchange(channel amqpChannel, exchangeName, queueName, routingKey string, headers amqp.Table) error {
	log.Printf("binding queue with name: %s to exchange: %s with routingkey: %s", queueName, exchangeName, routingKey)
	return channel.QueueBind(queueName, routingKey, exchangeName, false, headers)
}

func (c *connection) addHandler(queueName, routingKey string, handler interface{}) {
	err := checkMessageHandler(handler)
	if err != nil {
		c.addError(err)
		return
	}
	c.tryAddHandler(queueName, routingKey, "", handler)

}

func (c *connection) tryAddHandler(queueName, routingKey, serviceResponseExchangeName string, handler interface{}) {
	uniqueKey := queueRoutingKey{queue: queueName, routingKey: routingKey}
	handlerType := reflect.TypeOf(handler).Elem()
	if existing, exist := c.handlers[uniqueKey]; exist {
		existingType := reflect.TypeOf(existing.handler).Elem()
		c.addError(fmt.Errorf("routingkey %s for queue %s already assigned to handler %s, cannot assign %s", routingKey, queueName, existingType, handlerType))
		return
	}
	log.Printf("routingkey %s for queue %s assigned to handler %s", routingKey, queueName, handlerType)
	c.handlers[uniqueKey] = messageHandlerInvoker{handler: handler, queueRoutingKey: queueRoutingKey{queue: queueName, routingKey: routingKey}, responseExchange: serviceResponseExchangeName}
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

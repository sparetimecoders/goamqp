package go_amqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
	"time"
)

// TODO This is WIP and a lot of refactoring will happen!

// A Config that contains the necessary variables for connecting to RabbitMQ.
type Config struct {
	Username                string `env:"RABBITMQ_USERNAME,required"`
	Password                string `env:"RABBITMQ_PASSWORD,required"`
	Host                    string `env:"RABBITMQ_HOST,required"`
	Port                    int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	VHost                   string `env:"RABBITMQ_VHOST" envDefault:""`
	DelayedMessageSupported bool   `env:"RABBITMQ_DELAYED_MESSAGING" envDefault:"false"`
}

// IncomingMessageHandler is a marker interface for implementations that want to register as message handlers.
// Implementations MUST implement a Process method with a single argument of the type returned by the Type() method.
// The conditions will be checked during 'registration' of the handler.
// Example:
//
//  type IncomingMessageHandler struct {}
//
//  func (IncomingMessageHandler) Type() interface{} {
//	  return IncomingMessage{}
//  }
//
//  func (i IncomingMessageHandler) Process(m IncomingMessage) bool {
//	  return true
//  }
//
//  NewEventStreamListener("service", "key", IncomingMessageHandler{})
//
// When a message is received from RabbitMQ, a new instance of the type returned from Type() will be created and
// populated fromm the Json in the message.
// Process() is then called and if it returns true the message will be Acknowledged, otherwise it will be re-queued again.
type IncomingMessageHandler interface {
	Type() interface{}
}

// A DelayedMessage TODO
// The delayed messaging plugin must be installed on the RabbitMQ server to enable this functionality.
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
type DelayedMessage interface {
	TTL() time.Duration
}

// Connection is used to setup new listeners and publishers.
type Connection interface {
	// Create a new Event Stream Listener
	// The passed IncomingMessageHandler is used to process received messages
	NewEventStreamListener(svcName, routingKey string, handler IncomingMessageHandler) error
	// Create a new Service Listener (listening to <svcName>.request.queue)
	// The passed IncomingMessageHandler is used to process received messages
	NewServiceListener(svcName, routingKey string, handler IncomingMessageHandler) error
	// Create a new Event Stream Publisher
	// The returned Channel is used to put JSON "structs" onto the EventStream
	NewEventStreamPublisher(routingKey string) (chan interface{}, error)
	// Create a new Service publisher
	// The returned Channel is used to put JSON "structs" onto the EventStream
	NewServicePublisher(svcName, routingKey string) (chan interface{}, error)
}

// Internal state
type connection struct {
	connection       *amqp.Connection
	channel          *amqp.Channel
	delayedMessaging bool
}

var _ Connection = &connection{}
var eventsExchange = serviceExchangeName("events")

// Connect to a RabbitMQ instance
func NewFromUrl(amqpUrl string) (Connection, error) {
	conn, ch, err := connectToAmqpUrl(amqpUrl)
	return &connection{
		connection:       conn,
		channel:          ch,
		delayedMessaging: false,
	}, err
}

// Connect to a RabbitMQ instance
func New(config Config) (Connection, error) {
	conn, ch, err := connectToAmqpUrl(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.VHost))
	return &connection{
		connection:       conn,
		channel:          ch,
		delayedMessaging: config.DelayedMessageSupported,
	}, err
}

func connectToAmqpUrl(amqpUrl string) (*amqp.Connection, *amqp.Channel, error) {
	// TODO Filter password from URL
	log.Printf("connecting to %s", amqpUrl)

	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, nil
}

func (c connection) NewEventStreamListener(svcName, routingKey string, handler IncomingMessageHandler) error {
	invoker, err := checkHandler(handler)
	if err != nil {
		return err
	}
	msgs, err := c.eventListener(svcName, routingKey)
	if err != nil {
		return err
	}
	go listener(msgs, invoker, handler)
	return nil
}

func (c connection) NewServiceListener(svcName, routingKey string, handler IncomingMessageHandler) error {
	invoker, err := checkHandler(handler)
	if err != nil {
		return err
	}

	msgs, err := c.serviceListener(svcName, routingKey)
	if err != nil {
		return err
	}
	go listener(msgs, invoker, handler)
	return nil
}

func failOnError(err error, msg ...string) {
	if err != nil {
		log.Fatalf("%v %v", msg, err)
	}
}

func (c connection) NewEventStreamPublisher(routingKey string) (chan interface{}, error) {
	if err := c.setupEventExchange(); err != nil {
		return nil, err
	}
	p := make(chan interface{})
	go c.publisher(p, routingKey, eventsExchange)
	return p, nil
}

func (c connection) NewServicePublisher(svcName, routingKey string) (chan interface{}, error) {
	if err := c.setupServiceExchanges(svcName); err != nil {
		return nil, err
	}
	p := make(chan interface{})
	go c.publisher(p, routingKey, serviceExchangeName(svcName))
	return p, nil
}

func (c connection) publisher(p <-chan interface{}, routingKey, exchangeName string) {
	for msg := range p {
		log.Printf("publishing message to %s\n", exchangeName)
		jsonBytes, err := json.Marshal(msg)
		failOnError(err, "failed to transform json")

		err = c.channel.Publish(exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				Body:        jsonBytes,
				ContentType: "application/json",
			},
		)
		failOnError(err, "failed to publish event")
		log.Printf("published message to %s: %s", exchangeName, string(jsonBytes))
	}
}

func listener(msgs <-chan amqp.Delivery, invoker reflect.Value, handler IncomingMessageHandler) {
	fmt.Println("Started listener")
	for d := range msgs {
		log.Println("Received")
		message := parseMessage(d, handler)
		log.Println("Parsed")

		args := []reflect.Value{reflect.ValueOf(handler), reflect.ValueOf(message).Elem()}
		call := invoker.Call(args)[0]
		if call.Bool() {
			log.Printf("message [%s] handled successfully and will be ACKED", d.MessageId)
			_ = d.Ack(false)
		} else {
			log.Printf("message handler returned false, message [%s] will be NACKED", d.MessageId)
			_ = d.Nack(false, true)
		}
	}

}

func (c connection) eventListener(service, routingKey string) (<-chan amqp.Delivery, error) {
	if err := c.setupEventExchange(); err != nil {
		return nil, err
	}
	if _, err := c.declareEventQueue(service); err != nil {
		return nil, err
	}
	if err := c.bindToEventTopic(service, routingKey); err != nil {
		return nil, err
	}
	return c.consumeEventQueue(service)
}

func (c connection) serviceListener(service, routingKey string) (<-chan amqp.Delivery, error) {
	if err := c.setupServiceExchanges(service); err != nil {
		return nil, err
	}
	if _, err := c.declareServiceQueue(service); err != nil {
		return nil, err
	}
	if err := c.bindToService(service, routingKey); err != nil {
		return nil, err
	}
	return c.consumeRequestQueue(service)
}

func (c connection) setupEventExchange() error {
	return c.eventsExchange()
}

func (c connection) setupServiceExchanges(service string) error {
	if err := c.serviceRequestExchange(service); err != nil {
		return err
	}
	return c.serviceResponseExchange(service)
}

func parseMessage(delivery amqp.Delivery, handler IncomingMessageHandler) interface{} {
	body := delivery.Body
	res := reflect.New(reflect.TypeOf(handler.Type())).Elem().Addr().Interface()
	if err := json.Unmarshal(body, &res); err != nil {
		log.Fatalf("failed to deserialize json [%s]to struct, %v", string(body), err)
	}
	return res
}

func (c connection) eventsExchange() error {
	return c.exchangeDeclare(eventsExchange, "topic")
}

func (c connection) serviceRequestExchange(svcName string) error {
	return c.exchangeDeclare(serviceExchangeName(svcName), "direct")
}

func (c connection) serviceResponseExchange(svcName string) error {
	return c.exchangeDeclare(serviceExchangeName(svcName), "headers")
}

func serviceExchangeName(svcName string) string {
	return fmt.Sprintf("%s.topic.exchange", svcName)
}

func (c connection) exchangeDeclare(name, kind string) error {
	log.Printf("creating exchange with name %s", name)
	args := amqp.Table{}
	if c.delayedMessaging {
		args["x-delayed-type"] = kind
		kind = "x-delayed-message"
	}
	return c.channel.ExchangeDeclare(
		name,
		kind,
		true,
		false,
		false,
		false,
		args,
	)
}

func serviceEventQueueName(service string) string {
	return fmt.Sprintf("%s.queue.%s", eventsExchange, service)
}

func serviceRequestQueueName(service string) string {
	return fmt.Sprintf("%s.request.queue", service)
}

func (c connection) declareEventQueue(service string) (amqp.Queue, error) {
	return c.queueDeclare(serviceEventQueueName(service))
}

func (c connection) declareServiceQueue(service string) (amqp.Queue, error) {
	return c.queueDeclare(serviceRequestQueueName(service))
}

func (c connection) queueDeclare(name string) (amqp.Queue, error) {
	return c.channel.QueueDeclare(name,
		true,
		false,
		false,
		false,
		nil,
	)
}

func (c connection) bindToEventTopic(service, routingKey string) error {
	return c.channel.QueueBind(serviceEventQueueName(service), routingKey, eventsExchange, false, nil)
}

func (c connection) bindToService(service, routingKey string) error {
	return c.channel.QueueBind(serviceRequestQueueName(service), routingKey, serviceExchangeName(service), false, nil)
}

func (c connection) consumeEventQueue(service string) (<-chan amqp.Delivery, error) {
	return c.consume(serviceEventQueueName(service))
}

func (c connection) consumeRequestQueue(service string) (<-chan amqp.Delivery, error) {
	return c.consume(serviceRequestQueueName(service))
}

func (c connection) consume(queue string) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func checkHandler(handler IncomingMessageHandler) (reflect.Value, error) {
	m, ok := reflect.TypeOf(handler).MethodByName("Process")
	errValue := reflect.Value{}
	if !ok {
		return errValue, errors.New(fmt.Sprintf("missing method Process on handler, %s", reflect.TypeOf(handler).Elem()))
	}

	methodType := m.Type
	if methodType.NumIn() != 2 {
		return errValue, errors.New(fmt.Sprintf("incorrect number of arguments, expected 1 but was %d", methodType.NumIn()-1))
	}
	if methodType.In(1) != reflect.TypeOf(handler.Type()) {
		return errValue, errors.New(fmt.Sprintf("incorrect in arguments. Expected Process(%s), actual Process(%s)", reflect.TypeOf(handler.Type()), methodType.In(1)))
	}
	if methodType.NumOut() != 1 {
		return errValue, errors.New(fmt.Sprintf("incorrect number of return values. Expected 1, actual %d", methodType.NumOut()))
	}
	if methodType.Out(0).Kind() != reflect.Bool {
		return errValue, errors.New(fmt.Sprintf("incorrect return type for Process(%s). Expected bool, actual %v", reflect.TypeOf(handler.Type()), methodType.Out(0)))
	}
	return m.Func, nil
}

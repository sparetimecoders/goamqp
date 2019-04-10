package go_amqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
)

// TODO This is WIP and a lot of refactoring will happen!

// A Config that contains the necessary variables for connecting to RabbitMQ.
type Config struct {
	Username string `env:"RABBITMQ_USERNAME,required"`
	Password string `env:"RABBITMQ_PASSWORD,required"`
	Host     string `env:"RABBITMQ_HOST,required"`
	Port     int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	VHost    string `env:"RABBITMQ_VHOST" envDefault:""`
}

// IncomingMessageHandler is a marker interface for implementations that want to register as message handlers.
// Implementations MUST implement a Process method with a single argument of the type returned by the Type() method.
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

// Connection is used to setup new listeners and publishers.
type Connection interface {
	// Create a new Event Stream Listener
	// The passed MessageHandler is used to transform the incoming message from JSON and then process it
	NewEventStreamListener(svcName, routingKey string, handler IncomingMessageHandler)
	// Create a new Service Listener (listening to <svcName>.request.queue)
	// The passed MessageHandler is used to transform the incoming message from JSON and then process it
	NewServiceListener(svcName, routingKey string, handler IncomingMessageHandler)
	// Create a new Event Stream Publisher
	// The returned Channel is used to put JSON "structs" onto the EventStream
	NewEventStreamPublisher(routingKey string) chan interface{}
	// Create a new Service publisher
	// The returned Channel is used to put JSON "structs" onto the EventStream
	NewServicePublisher(svcName, routingKey string) chan interface{}
}

// Internal state
type connection struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

var _ Connection = &connection{}

func NewFromUrl(amqpUrl string) (Connection, error) {
	return connectToAmqp(amqpUrl)
}

// Connect to a RabbitMQ instance
func New(config Config) (Connection, error) {
	return connectToAmqp(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.VHost))
}

// Create a new Event Stream Listener
// The passed IncomingMessageHandler will be called with the message after deserialization
func (c connection) NewEventStreamListener(svcName, routingKey string, handler IncomingMessageHandler) {
	invoker, err := checkHandler(handler)
	if err != nil {
		log.Fatal(err)
	}
	msgs := c.eventListener(svcName, routingKey)
	go listener(msgs, invoker, handler)
}

func (c connection) NewServiceListener(svcName, routingKey string, handler IncomingMessageHandler) {
	invoker, err := checkHandler(handler)
	if err != nil {
		log.Fatal(err)
	}
	msgs := c.serviceListener(svcName, routingKey)
	go listener(msgs, invoker, handler)
}

func (c connection) NewEventStreamPublisher(routingKey string) chan interface{} {
	c.setupEventExchange()
	p := make(chan interface{})
	go c.publisher(p, routingKey, eventExchangeName)
	return p
}

func (c connection) NewServicePublisher(svcName, routingKey string) chan interface{} {
	c.setupServiceExchange(svcName)
	p := make(chan interface{})
	go c.publisher(p, routingKey, serviceExchangeName(svcName))
	return p
}

func (c connection) publisher(p <-chan interface{}, routingKey, exchangeName string) {
	for msg := range p {
		log.Println("publishing message to event stream")
		jsonBytes, err := json.Marshal(msg)
		if err != nil {
			log.Fatal("failed to transform json", err)
		}
		err = c.channel.Publish(exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				Body:        jsonBytes,
				ContentType: "application/json",
			},
		)
		if err != nil {
			log.Fatal("failed to publish event", err)
		}
		log.Println("published message to event stream", string(jsonBytes))
	}
}

func listener(msgs <-chan amqp.Delivery, invoker reflect.Value, handler IncomingMessageHandler) {
	for d := range msgs {
		message := parseMessage(d, handler)

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

func connectToAmqp(amqpUrl string) (Connection, error) {
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &connection{
		connection: conn,
		channel:    ch,
	}, nil
}

func (c connection) eventListener(service, routingKey string) <-chan amqp.Delivery {
	// TODO Errorhandling
	c.setupEventExchange()
	_, _ = c.declareEventQueue(service)
	_ = c.bindToEventTopic(service, routingKey)
	delivery, _ := c.consumeEventQueue(service)
	return delivery
}

func (c connection) serviceListener(service, routingKey string) <-chan amqp.Delivery {
	// TODO Errorhandling
	c.setupServiceExchange(service)
	_, _ = c.declareServiceQueue(service)
	_ = c.bindToService(service, routingKey)
	delivery, _ := c.consumeRequestQueue(service)
	return delivery
}

func (c connection) setupEventExchange() {
	// TODO Errorhandling
	_ = c.eventsExchange()
}

func (c connection) setupServiceExchange(service string) {
	// TODO Errorhandling
	_ = c.serviceExchange(service)
}

func parseMessage(delivery amqp.Delivery, handler IncomingMessageHandler) interface{} {
	body := delivery.Body
	res := reflect.New(reflect.TypeOf(handler.Type())).Elem().Addr().Interface()
	if err := json.Unmarshal(body, &res); err != nil {
		log.Fatalf("failed to deserialize json [%s]to struct, %v", string(body), err)
	}
	return res
}

var eventExchangeName = "events.topic.exchange"

func (c connection) eventsExchange() error {
	return c.exchangeDeclare(eventExchangeName)
}

func (c connection) serviceExchange(svcName string) error {
	return c.exchangeDeclare(serviceExchangeName(svcName))
}

func serviceExchangeName(svcName string) string {
	return fmt.Sprintf("%s.topic.exchange", svcName)
}

func (c connection) exchangeDeclare(name string) error {
	return c.channel.ExchangeDeclare(
		name,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
}

func serviceEventQueueName(service string) string {
	return fmt.Sprintf("%s.queue.%s", eventExchangeName, service)
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
	return c.channel.QueueBind(serviceEventQueueName(service), routingKey, eventExchangeName, false, nil)
}

func (c connection) bindToService(service, routingKey string) error {
	return c.channel.QueueBind(serviceRequestQueueName(service), routingKey, "", false, nil)
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

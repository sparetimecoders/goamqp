package go_amqp

import (
	"encoding/json"
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

// A new MessageHandler will created and populated from the amqp.Delivery.Body when a new message has been received on the queue
// Handle() is then called and if it returns true the message will be Acknowledged, otherwise it will be re-queued again.
type MessageHandler interface {
	Handle() bool
}

// Connection is used to setup new listeners and publishers.
type Connection interface {
	NewEventStreamListener(svcName, routingKey string, handler MessageHandler)
	NewEventStreamPublisher(routingKey string) chan interface{}
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
// The passed MessageHandler is used to transform the incoming message from JSON and then process it
func (c connection) NewEventStreamListener(svcName, routingKey string, handler MessageHandler) {
	msgs := listener(svcName, routingKey, c.channel)

	go func() {
		for d := range msgs {
			if parseMessage(d, handler).Handle() {
				log.Printf("message [%s] handled successfully and will be ACKED", d.MessageId)
				_ = d.Ack(false)
			} else {
				log.Printf("message handler returned false, message [%s] will be NACKED", d.MessageId)
				_ = d.Nack(false, true)
			}
		}
	}()
}

// Create a new Event Stream Publisher
// The returned Channel is used to put JSON "structs" onto the EventStream
func (c connection) NewEventStreamPublisher(routingKey string) chan interface{} {
	p := make(chan interface{})

	go func() {
		for msg := range p {
			jsonStr, _ := json.Marshal(msg)
			_ = c.channel.Publish(eventExchangeName,
				routingKey,
				false,
				false,
				amqp.Publishing{
					Body:        jsonStr,
					ContentType: "application/json",
				},
			)
		}

	}()
	return p
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

func listener(service string, routingKey string, ch *amqp.Channel) <-chan amqp.Delivery {
	// TODO Errorhandling
	_ = eventsExchange(ch)
	_, _ = declareEventQueue(service, ch)
	_ = bindToEventTopic(service, routingKey, ch)
	delivery, _ := consume(service, ch)
	return delivery
}

func parseMessage(delivery amqp.Delivery, handler MessageHandler) MessageHandler {
	body := delivery.Body
	res := reflect.New(reflect.TypeOf(handler)).Elem().Addr().Interface()
	if err := json.Unmarshal(body, &res); err != nil {
		log.Fatalf("failed to deserialize json [%s]to struct, %v", string(body), err)
	}
	return res.(MessageHandler)
}

var eventExchangeName = "events.topic.exchange"

func eventsExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(eventExchangeName,
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

func declareEventQueue(service string, ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(serviceEventQueueName(service),
		true,
		false,
		false,
		false,
		nil,
	)
}

func bindToEventTopic(service, routingKey string, ch *amqp.Channel) error {
	return ch.QueueBind(serviceEventQueueName(service), routingKey, eventExchangeName, false, nil)
}

func consume(service string, ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		serviceEventQueueName(service),
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

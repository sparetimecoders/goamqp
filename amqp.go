package go_amqp

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Config struct {
	Username string `env:"RABBITMQ_USERNAME,required"`
	Password string `env:"RABBITMQ_PASSWORD,required"`
	Host     string `env:"RABBITMQ_HOST,required"`
	Port     int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	VHost    string `env:"RABBITMQ_VHOST" envDefault:""`
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

func listener(service string, routingKey string, ch *amqp.Channel) <-chan amqp.Delivery {
	// TODO Errorhandling
	eventsExchange(ch)
	declareEventQueue(service, ch)
	bindToEventTopic(service, routingKey, ch)
	delivery, _ := consume(service, ch)
	return delivery
}

func Connect(config Config) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.VHost))

}
func NewListener(svcName, routingKey string, onMessage func(delivery amqp.Delivery), connection *amqp.Connection) {

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalln("failed to open channel", err)
	}
	msgs := listener(svcName, routingKey, ch)

	go func() {
		for d := range msgs {
			onMessage(d)
		}
	}()

}

func NewEventPublisher(routingKey string, connection *amqp.Connection) chan string {
	p := make(chan string)
	ch, err := connection.Channel()
	if err != nil {
		log.Fatalln("failed to open channel", err)
	}

	go func() {
		for msg := range p {
			ch.Publish(eventExchangeName,
				routingKey,
				false,
				false,
				amqp.Publishing{
					Body:        []byte(msg),
					ContentType: "application/json",
				},
			)
		}

	}()
	return p
}

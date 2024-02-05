package goamqp

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// EventStreamConsumer sets up ap a durable, persistent event stream consumer.
// For a transient queue, use the TransientEventStreamConsumer function instead.
func EventStreamConsumer[T any](routingKey string, handler EventHandler[T], opts ...QueueBindingConfigSetup) Setup {
	return StreamConsumer(defaultEventExchangeName, routingKey, handler, opts...)
}

// ServiceResponseConsumer is a specialization of EventStreamConsumer
// It sets up ap a durable, persistent consumer (exchange->queue) for responses from targetService
func ServiceResponseConsumer[T any](targetService, routingKey string, handler EventHandler[T]) Setup {
	return func(c *Connection) error {
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      newWrappedHandler(handler),
			queueName:    serviceResponseQueueName(targetService, c.serviceName),
			exchangeName: serviceResponseExchangeName(targetService),
			kind:         kindHeaders,
			headers:      amqp.Table{headerService: c.serviceName},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// ServiceRequestConsumer is a specialization of EventStreamConsumer
// It sets up ap a durable, persistent consumer (exchange->queue) for message to the service owning the Connection
func ServiceRequestConsumer[T any](routingKey string, handler EventHandler[T]) Setup {
	return func(c *Connection) error {
		resExchangeName := serviceResponseExchangeName(c.serviceName)
		if err := c.exchangeDeclare(c.channel, resExchangeName, kindHeaders); err != nil {
			return fmt.Errorf("failed to create exchange %s, %w", resExchangeName, err)
		}

		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      newWrappedHandler(handler),
			queueName:    serviceRequestQueueName(c.serviceName),
			exchangeName: serviceRequestExchangeName(c.serviceName),
			kind:         kindDirect,
			headers:      amqp.Table{},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// StreamConsumer sets up ap a durable, persistent event stream consumer.
func StreamConsumer[T any](exchange, routingKey string, handler EventHandler[T], opts ...QueueBindingConfigSetup) Setup {
	exchangeName := topicExchangeName(exchange)
	return func(c *Connection) error {
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      newWrappedHandler(handler),
			queueName:    serviceEventQueueName(exchangeName, c.serviceName),
			exchangeName: exchangeName,
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

// TransientEventStreamConsumer sets up an event stream consumer that will clean up resources when the
// connection is closed.
// For a durable queue, use the EventStreamConsumer function instead.
func TransientEventStreamConsumer[T any](routingKey string, handler EventHandler[T]) Setup {
	return TransientStreamConsumer(defaultEventExchangeName, routingKey, handler)
}

// TransientStreamConsumer sets up an event stream consumer that will clean up resources when the
// connection is closed.
// For a durable queue, use the StreamConsumer function instead.
func TransientStreamConsumer[T any](exchange, routingKey string, handler EventHandler[T]) Setup {
	exchangeName := topicExchangeName(exchange)
	return func(c *Connection) error {
		queueName := serviceEventRandomQueueName(exchangeName, c.serviceName)
		if err := c.addHandler(queueName, routingKey, newWrappedHandler(handler)); err != nil {
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

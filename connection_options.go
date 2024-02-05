// MIT License
//
// Copyright (c) 2024 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package goamqp

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Setup is a setup function that takes a Connection and use it to set up AMQP
// An example is to create exchanges and queues
type Setup func(conn *Connection) error

// WithTypeMapping adds a two-way mapping between a type and a routing key. The mapping needs to be unique.
func WithTypeMapping(routingKey string, msgType any) Setup {
	return func(conn *Connection) error {
		typ := reflect.TypeOf(msgType)
		if t, exists := conn.keyToType[routingKey]; exists {
			return fmt.Errorf("mapping for routing key '%s' already registered to type '%s'", routingKey, t)
		}
		if key, exists := conn.typeToKey[typ]; exists {
			return fmt.Errorf("mapping for type '%s' already registered to routing key '%s'", typ, key)
		}
		conn.keyToType[routingKey] = typ
		conn.typeToKey[typ] = routingKey
		return nil
	}
}

func WithHandler[T any](routingKey string, handler EventHandler[T]) Setup {
	exchangeName := topicExchangeName(defaultEventExchangeName)
	return func(c *Connection) error {
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      newWrappedHandler(handler),
			queueName:    serviceEventQueueName(exchangeName, c.serviceName),
			exchangeName: exchangeName,
			kind:         kindTopic,
			headers:      amqp.Table{},
		}

		return c.messageHandlerBindQueueToExchange(config)
	}
}

// WithPrefetchLimit configures the number of messages to prefetch from the server.
// To get round-robin behavior between consumers consuming from the same queue on
// different connections, set the prefetch count to 1, and the next available
// message on the server will be delivered to the next available consumer.
// If your consumer work time is reasonably consistent and not much greater
// than two times your network round trip time, you will see significant
// throughput improvements starting with a prefetch count of 2 or slightly
// greater, as described by benchmarks on RabbitMQ.
//
// http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
func WithPrefetchLimit(limit int) Setup {
	return func(conn *Connection) error {
		return conn.channel.Qos(limit, 0, true)
	}
}

// WithNotificationChannel specifies a go channel to receive messages
// such as connection established, reconnecting, event published, consumed, etc.
func WithNotificationChannel(notificationCh chan<- Notification) Setup {
	return func(conn *Connection) error {
		conn.notificationCh = notificationCh
		return nil
	}
}

// TODO REMOVE and use WithNotificationChannel instead?
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

// TransientEventStreamConsumer sets up an event stream consumer that will clean up resources when the
// connection is closed.
// For a durable queue, use the EventStreamConsumer function instead.
func TransientEventStreamConsumer[T any](routingKey string, handler EventHandler[T]) Setup {
	return TransientStreamConsumer(defaultEventExchangeName, routingKey, handler)
}

// EventStreamConsumer sets up ap a durable, persistent event stream consumer.
// For a transient queue, use the TransientEventStreamConsumer function instead.
func EventStreamConsumer[T any](routingKey string, handler EventHandler[T], opts ...QueueBindingConfigSetup) Setup {
	return StreamConsumer(defaultEventExchangeName, routingKey, handler, opts...)
}

// EventStreamPublisher sets up an event stream publisher
func EventStreamPublisher(publisher *Publisher) Setup {
	return StreamPublisher(defaultEventExchangeName, publisher)
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

// StreamPublisher sets up an event stream publisher
func StreamPublisher(exchange string, publisher *Publisher) Setup {
	name := topicExchangeName(exchange)
	return func(c *Connection) error {
		if err := c.exchangeDeclare(c.channel, name, kindTopic); err != nil {
			return errors.Wrapf(err, "failed to declare exchange %s", name)
		}
		publisher.connection = c
		if err := publisher.setDefaultHeaders(c.serviceName); err != nil {
			return err
		}
		publisher.exchange = name
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
			Header{Key: "CC", Value: []any{destinationQueueName}},
		); err != nil {
			return err
		}
		publisher.exchange = ""
		return nil
	}
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
			return errors.Wrapf(err, "failed to create exchange %s", resExchangeName)
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

// ServicePublisher sets up ap a publisher, that sends messages to the targetService
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

// RequestResponseHandler is a convenience func to set up ServiceRequestConsumer and combines it with
// PublishServiceResponse
func RequestResponseHandler[T any](routingKey string, handler EventHandler[T]) Setup {
	return func(c *Connection) error {
		responseHandlerWrapper := responseWrapper(handler, routingKey, c.PublishServiceResponse)
		return ServiceRequestConsumer[T](routingKey, responseHandlerWrapper)(c)
	}
}

// PublishNotify see amqp.Channel.Confirm
func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.channel.NotifyPublish(confirm)
		return c.channel.Confirm(false)
	}
}

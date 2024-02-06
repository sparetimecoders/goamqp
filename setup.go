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
	"errors"
	"fmt"
	"reflect"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RoutingKeyToType map[string]reflect.Type

type TypeToRoutingKey map[reflect.Type]string

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

// PublishNotify see amqp.Channel.Confirm
func PublishNotify(confirm chan amqp.Confirmation) Setup {
	return func(c *Connection) error {
		c.channel.NotifyPublish(confirm)
		return c.channel.Confirm(false)
	}
}

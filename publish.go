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
	"context"
	"fmt"
	"reflect"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is used to send messages
type Publisher struct {
	connection     *Connection
	exchange       string
	defaultHeaders []Header
}

// ErrNoRouteForMessageType when the published message cannot be routed.
var ErrNoRouteForMessageType = fmt.Errorf("no routingkey configured for message of type")

// NewPublisher returns a publisher that can be used to send messages
func NewPublisher() *Publisher {
	return &Publisher{}
}

func (p *Publisher) Publish(ctx context.Context, msg any, headers ...Header) error {
	table := amqp.Table{}
	for _, v := range p.defaultHeaders {
		table[v.Key] = v.Value
	}
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
		table[h.Key] = h.Value
	}

	t := reflect.TypeOf(msg)
	key := t
	if t.Kind() == reflect.Ptr {
		key = t.Elem()
	}
	if key, ok := p.connection.typeToKey[key]; ok {
		return p.connection.publishMessage(ctx, msg, key, p.exchange, table)
	}
	return fmt.Errorf("%w %s", ErrNoRouteForMessageType, t)
}

// EventStreamPublisher sets up an event stream publisher
func EventStreamPublisher(publisher *Publisher) Setup {
	return StreamPublisher(defaultEventExchangeName, publisher)
}

// StreamPublisher sets up an event stream publisher
func StreamPublisher(exchange string, publisher *Publisher) Setup {
	name := topicExchangeName(exchange)
	return func(c *Connection) error {
		if err := c.exchangeDeclare(c.channel, name, kindTopic); err != nil {
			return fmt.Errorf("failed to declare exchange %s, %w", name, err)
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

func (p *Publisher) setDefaultHeaders(serviceName string, headers ...Header) error {
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	p.defaultHeaders = append(headers, Header{Key: headerService, Value: serviceName})
	return nil
}

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
	"encoding/json"
	"fmt"
	"reflect"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is used to send messages
type Publisher struct {
	typeToKey      typeToRoutingKey
	channel        AmqpChannel
	exchange       string
	defaultHeaders []Header
}

// ErrNoRouteForMessageType when the published message cannot be routed.
var (
	ErrNoRouteForMessageType    = fmt.Errorf("no routingkey configured for message of type")
	ErrNoMessageTypeForRouteKey = fmt.Errorf("no message type for routingkey configured")
)

// NewPublisher returns a publisher that can be used to send messages
func NewPublisher() *Publisher {
	return &Publisher{}
}

// PublishWithContext wraps Publish to ease migration to new version of goamqp
// Deprecated: use Publish directly
func (p *Publisher) PublishWithContext(ctx context.Context, msg any, headers ...Header) error {
	return p.Publish(ctx, msg, headers...)
}

// Publish tries to publish msg to AMQP.
// It requires RoutingKey <-> Type mappings from WithTypeMapping in order to set the correct Routing Key for msg
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
	if key, ok := p.typeToKey[key]; ok {
		return publishMessage(ctx, p.channel, msg, key, p.exchange, table)
	}
	return fmt.Errorf("%w %s", ErrNoRouteForMessageType, t)
}

// EventStreamPublisher sets up an event stream publisher
func EventStreamPublisher(publisher *Publisher) Setup {
	return StreamPublisher(defaultEventExchangeName, publisher)
}

// StreamPublisher sets up an event stream publisher
func StreamPublisher(exchange string, publisher *Publisher) Setup {
	exchangeName := topicExchangeName(exchange)
	return func(c *Connection) error {
		if err := exchangeDeclare(c.channel, exchangeName, amqp.ExchangeTopic); err != nil {
			return fmt.Errorf("failed to declare exchange %s, %w", exchangeName, err)
		}
		return publisher.setup(c.channel, c.serviceName, exchangeName, c.typeToKey)
	}
}

// QueuePublisher sets up a publisher that will send events to a specific queue instead of using the exchange,
// so called Sender-Selected distribution
// https://www.rabbitmq.com/sender-selected.html#:~:text=The%20RabbitMQ%20broker%20treats%20the,key%20if%20they%20are%20present.
func QueuePublisher(publisher *Publisher, destinationQueueName string) Setup {
	return func(c *Connection) error {
		return publisher.setup(c.channel, c.serviceName, "", c.typeToKey, Header{Key: "CC", Value: []any{destinationQueueName}})
	}
}

// ServicePublisher sets up ap a publisher, that sends messages to the targetService
func ServicePublisher(targetService string, publisher *Publisher) Setup {
	exchangeName := serviceRequestExchangeName(targetService)
	return func(c *Connection) error {
		if err := exchangeDeclare(c.channel, exchangeName, amqp.ExchangeDirect); err != nil {
			return err
		}
		return publisher.setup(c.channel, c.serviceName, exchangeName, c.typeToKey)
	}
}

func (p *Publisher) setup(channel AmqpChannel, serviceName, exchange string, typeToKey typeToRoutingKey, headers ...Header) error {
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	p.defaultHeaders = append(headers, Header{Key: headerService, Value: serviceName})
	p.channel = channel
	p.typeToKey = typeToKey
	p.exchange = exchange
	return nil
}

func publishMessage(ctx context.Context, channel AmqpChannel, msg any, routingKey, exchangeName string, headers amqp.Table) error {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		Body:         jsonBytes,
		ContentType:  contentType,
		DeliveryMode: 2,
		Headers:      injectToHeaders(ctx, headers),
	}
	err = channel.PublishWithContext(ctx, exchangeName,
		routingKey,
		false,
		false,
		publishing,
	)
	if err != nil {
		eventPublishFailed(exchangeName, routingKey)
		return err
	}
	eventPublishSucceed(exchangeName, routingKey)
	return nil
}

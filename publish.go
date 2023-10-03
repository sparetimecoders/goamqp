// MIT License
//
// Copyright (c) 2019 sparetimecoders
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

var (
	// ErrNoRouteForMessageType when the published message cannot be routed.
	ErrNoRouteForMessageType = fmt.Errorf("no routingkey configured for message of type")
)

// NewPublisher returns a publisher that can be used to send messages
func NewPublisher() *Publisher {
	return &Publisher{}
}

// Publish publishes a message to a given exchange
// Deprecated: Use PublishWithContext instead.
func (p *Publisher) Publish(msg any, headers ...Header) error {
	return p.PublishWithContext(context.Background(), msg, headers...)
}

func (p *Publisher) PublishWithContext(ctx context.Context, msg any, headers ...Header) error {
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

func (p *Publisher) setDefaultHeaders(serviceName string, headers ...Header) error {
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	p.defaultHeaders = append(headers, Header{Key: headerService, Value: serviceName})
	return nil
}

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
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_Publisher_Setups(t *testing.T) {
	// Needed for transient stream tests
	uuid.SetRand(badRand{})

	tests := []struct {
		name              string
		opts              func(p *Publisher) []Setup
		messages          []any
		expectedError     string
		expectedExchanges []ExchangeDeclaration
		expectedQueues    []QueueDeclaration
		expectedBindings  []BindingDeclaration
		expectedPublished []*Publish
		headers           []Header
	}{
		{
			name: "EventStreamConsumer",
			opts: func(p *Publisher) []Setup {
				return []Setup{QueuePublisher(p, "destQueue"), WithTypeMapping("key", TestMessage{})}
			},
			messages: []any{TestMessage{"test", true}},
			headers:  []Header{{"x-header", "header"}},
			expectedPublished: []*Publish{{
				exchange:  "",
				key:       "key",
				mandatory: false,
				immediate: false,
				msg: amqp.Publishing{
					Headers:         amqp.Table{"CC": []interface{}{"destQueue"}, "service": "svc", "x-header": "header"},
					ContentType:     contentType,
					ContentEncoding: "",
					DeliveryMode:    2,
				},
			}},
		},
		{
			name: "EventStreamPublisher",
			opts: func(p *Publisher) []Setup {
				return []Setup{EventStreamPublisher(p), WithTypeMapping("key", TestMessage{})}
			},
			messages:          []any{TestMessage{"test", true}},
			headers:           []Header{{"x-header", "header"}},
			expectedExchanges: []ExchangeDeclaration{{name: topicExchangeName(defaultEventExchangeName), noWait: false, internal: false, autoDelete: false, durable: true, kind: kindTopic, args: nil}},
			expectedPublished: []*Publish{{
				exchange:  topicExchangeName(defaultEventExchangeName),
				key:       "key",
				mandatory: false,
				immediate: false,
				msg: amqp.Publishing{
					Headers:         amqp.Table{"service": "svc", "x-header": "header"},
					ContentType:     contentType,
					ContentEncoding: "",
					DeliveryMode:    2,
				},
			}},
		},
		{
			name: "ServicePublisher",
			opts: func(p *Publisher) []Setup {
				return []Setup{ServicePublisher("svc", p), WithTypeMapping("key", TestMessage{})}
			},
			expectedExchanges: []ExchangeDeclaration{{name: serviceRequestExchangeName("svc"), noWait: false, internal: false, autoDelete: false, durable: true, kind: kindDirect, args: nil}},
			messages:          []any{TestMessage{"test", true}},
			expectedPublished: []*Publish{{
				exchange:  serviceRequestExchangeName("svc"),
				key:       "key",
				mandatory: false,
				immediate: false,
				msg: amqp.Publishing{
					Headers:         amqp.Table{"service": "svc"},
					ContentType:     contentType,
					ContentEncoding: "",
					DeliveryMode:    2,
				},
			}},
		},
		{
			name: "ServicePublisher - multiple",
			opts: func(p *Publisher) []Setup {
				return []Setup{ServicePublisher("svc", p), WithTypeMapping("key1", TestMessage{}), WithTypeMapping("key2", TestMessage2{})}
			},
			expectedExchanges: []ExchangeDeclaration{{name: serviceRequestExchangeName("svc"), noWait: false, internal: false, autoDelete: false, durable: true, kind: kindDirect, args: nil}},
			messages: []any{
				TestMessage{"test", true},
				TestMessage2{"test", false},
				TestMessage{"test", false},
			},
			expectedPublished: []*Publish{
				{
					exchange:  serviceRequestExchangeName("svc"),
					key:       "key1",
					mandatory: false,
					immediate: false,
					msg: amqp.Publishing{
						Headers:         amqp.Table{"service": "svc"},
						ContentType:     contentType,
						ContentEncoding: "",
						DeliveryMode:    2,
					},
				}, {
					exchange:  serviceRequestExchangeName("svc"),
					key:       "key2",
					mandatory: false,
					immediate: false,
					msg: amqp.Publishing{
						Headers:         amqp.Table{"service": "svc"},
						ContentType:     contentType,
						ContentEncoding: "",
						DeliveryMode:    2,
					},
				}, {
					exchange:  serviceRequestExchangeName("svc"),
					key:       "key1",
					mandatory: false,
					immediate: false,
					msg: amqp.Publishing{
						Headers:         amqp.Table{"service": "svc"},
						ContentType:     contentType,
						ContentEncoding: "",
						DeliveryMode:    2,
					},
				},
			},
		},
		{
			name: "no route",
			opts: func(p *Publisher) []Setup {
				return []Setup{ServicePublisher("svc", p)}
			},
			expectedExchanges: []ExchangeDeclaration{{name: serviceRequestExchangeName("svc"), noWait: false, internal: false, autoDelete: false, durable: true, kind: kindDirect, args: nil}},
			messages: []any{
				TestMessage{"test", true},
			},
			expectedError: "no routingkey configured for message of type goamqp.TestMessage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel := NewMockAmqpChannel()
			conn := mockConnection(channel)
			p := NewPublisher()
			ctx := context.TODO()
			startErr := conn.Start(context.Background(), tt.opts(p)...)
			require.NoError(t, startErr)
			if tt.expectedExchanges != nil {
				require.Equal(t, tt.expectedExchanges, channel.ExchangeDeclarations)
			} else {
				require.Len(t, channel.ExchangeDeclarations, 0)
			}
			if tt.expectedQueues != nil {
				require.Equal(t, tt.expectedQueues, channel.QueueDeclarations)
			} else {
				require.Len(t, channel.QueueDeclarations, 0)
			}
			if tt.expectedBindings != nil {
				require.Equal(t, tt.expectedBindings, channel.BindingDeclarations)
			} else {
				require.Len(t, channel.BindingDeclarations, 0)
			}

			for i, msg := range tt.messages {
				err := p.Publish(ctx, msg, tt.headers...)
				if tt.expectedError != "" {
					require.ErrorContains(t, err, tt.expectedError)
					continue
				} else {
					require.NoError(t, err)
				}
				if tt.expectedPublished[i] != nil {
					body, err := json.Marshal(msg)
					require.NoError(t, err)
					tt.expectedPublished[i].msg.Body = body
					require.Equal(t, *tt.expectedPublished[i], <-channel.Published)
				} else if tt.expectedError == "" {
					require.Fail(t, "nothing published, and no error wanted!")
				}
			}
		})
	}
}

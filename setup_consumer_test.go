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
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Consumer_Setups(t *testing.T) {
	// Needed for transient stream tests
	uuid.SetRand(badRand{})

	tests := []struct {
		name              string
		opts              []Setup
		expectedError     string
		expectedExchanges []ExchangeDeclaration
		expectedQueues    []QueueDeclaration
		expectedBindings  []BindingDeclaration
		expectedConsumer  []Consumer
	}{
		{
			name: "EventStreamConsumer",
			opts: []Setup{EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return nil
			})},
			expectedExchanges: []ExchangeDeclaration{{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: nil}},
			expectedQueues:    []QueueDeclaration{{name: "events.topic.exchange.queue.svc", noWait: false, autoDelete: false, durable: true, exclusive: false, args: defaultQueueOptions}},
			expectedBindings:  []BindingDeclaration{{queue: "events.topic.exchange.queue.svc", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}},
			expectedConsumer:  []Consumer{{queue: "events.topic.exchange.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: nil}},
		},
		{
			name: "EventStreamConsumer with suffix",
			opts: []Setup{EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return nil
			}, AddQueueNameSuffix("suffix"))},
			expectedExchanges: []ExchangeDeclaration{{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: nil}},
			expectedQueues:    []QueueDeclaration{{name: "events.topic.exchange.queue.svc-suffix", noWait: false, autoDelete: false, durable: true, exclusive: false, args: defaultQueueOptions}},
			expectedBindings:  []BindingDeclaration{{queue: "events.topic.exchange.queue.svc-suffix", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}},
			expectedConsumer:  []Consumer{{queue: "events.topic.exchange.queue.svc-suffix", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: nil}},
		},
		{
			name: "EventStreamConsumer with empty suffix - fails",
			opts: []Setup{EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return nil
			}, AddQueueNameSuffix(""))},
			expectedError: "failed, empty queue suffix not allowed",
		},
		{
			name: "ServiceRequestConsumer",
			opts: []Setup{ServiceRequestConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return nil
			})},
			expectedExchanges: []ExchangeDeclaration{
				{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: nil},
				{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: nil},
			},
			expectedQueues:   []QueueDeclaration{{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, exclusive: false, args: defaultQueueOptions}},
			expectedBindings: []BindingDeclaration{{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}},
			expectedConsumer: []Consumer{{queue: "svc.direct.exchange.request.queue", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: nil}},
		},
		{
			name: "ServiceResponseConsumer",
			opts: []Setup{ServiceResponseConsumer("targetService", "key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return nil
			})},
			expectedExchanges: []ExchangeDeclaration{{name: "targetService.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: nil}},
			expectedQueues:    []QueueDeclaration{{name: "targetService.headers.exchange.response.queue.svc", noWait: false, autoDelete: false, durable: true, exclusive: false, args: defaultQueueOptions}},
			expectedBindings:  []BindingDeclaration{{queue: "targetService.headers.exchange.response.queue.svc", noWait: false, exchange: "targetService.headers.exchange.response", key: "key", args: amqp.Table{headerService: "svc"}}},
			expectedConsumer:  []Consumer{{queue: "targetService.headers.exchange.response.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: nil}},
		},
		{
			name: "TransientEventStreamConsumer",
			opts: []Setup{TransientEventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
				return errors.New("failed")
			})},
			expectedExchanges: []ExchangeDeclaration{{name: "events.topic.exchange", kind: "topic", durable: true, autoDelete: false, internal: false, noWait: false, args: nil}},
			expectedBindings:  []BindingDeclaration{{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", key: "key", exchange: "events.topic.exchange", noWait: false, args: amqp.Table{}}},
			expectedQueues: []QueueDeclaration{{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: true, autoDelete: false, exclusive: false, noWait: false, args: func() map[string]any {
				clone := maps.Clone(defaultQueueOptions)
				clone[amqp.QueueTTLArg] = 1000
				return clone
			}()}},
			expectedConsumer: []Consumer{{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: nil}},
		},
		{
			name: "routing key already exists",
			opts: []Setup{
				TransientEventStreamConsumer("root.key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
					return errors.New("failed")
				}), TransientEventStreamConsumer("root.#", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
					return errors.New("failed")
				}),
			},
			expectedExchanges: []ExchangeDeclaration{{name: "events.topic.exchange", kind: "topic", durable: true, autoDelete: false, internal: false, noWait: false, args: nil}},
			expectedBindings:  []BindingDeclaration{{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", key: "root.key", exchange: "events.topic.exchange", noWait: false, args: amqp.Table{}}},
			expectedQueues: []QueueDeclaration{{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: true, autoDelete: false, exclusive: false, noWait: false, args: func() map[string]any {
				clone := maps.Clone(defaultQueueOptions)
				clone[amqp.QueueTTLArg] = 1000
				return clone
			}()}},
			expectedError: "routingkey root.# overlaps root.key for queue events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f, consider using AddQueueNameSuffix",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel := NewMockAmqpChannel()
			conn := mockConnection(channel)
			err := conn.Start(context.Background(), tt.opts...)

			if tt.expectedConsumer != nil {
				require.Equal(t, tt.expectedConsumer, channel.Consumers)
			} else {
				require.Len(t, channel.Consumers, 0)
			}
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
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_MappingsInContext(t *testing.T) {
	mappings := routingKeyToType{
		"string": reflect.TypeOf(""),
		"double": reflect.TypeOf(1.0),
	}
	rootCtx := context.TODO()
	ctx := injectRoutingKeyToTypeContext(rootCtx, mappings)
	instance, ok := routingKeyToTypeFromContext(ctx, "string")
	require.True(t, ok)
	require.IsType(t, reflect.TypeOf(instance), reflect.TypeOf(""))

	_, ok = routingKeyToTypeFromContext(ctx, "int")
	require.False(t, ok)

	// This should always fail
	_, ok = routingKeyToTypeFromContext(rootCtx, "string")
	require.False(t, ok)
}

func Test_TypeMappingHandler(t *testing.T) {
	type fields struct {
		keyToType map[string]reflect.Type
	}
	type args struct {
		handler func(t *testing.T) Handler
		msg     json.RawMessage
		key     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "no mapped type, ignored",
			fields: fields{},
			args: args{
				msg: []byte(`{"a":true}`),
				key: "unknown",
				handler: func(t *testing.T) Handler {
					return func(ctx context.Context, event ConsumableEvent[any]) error {
						return nil
					}
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrNoMessageTypeForRouteKey)
			},
		},
		{
			name: "parse error",
			fields: fields{
				keyToType: map[string]reflect.Type{
					"known": reflect.TypeOf(TestMessage{}),
				},
			},
			args: args{
				msg: []byte(`{"a:}`),
				key: "known",
				handler: func(t *testing.T) Handler {
					return func(ctx context.Context, event ConsumableEvent[any]) error {
						return nil
					}
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "unexpected end of JSON input")
			},
		},
		{
			name: "handler error",
			fields: fields{
				keyToType: map[string]reflect.Type{
					"known": reflect.TypeOf(TestMessage{}),
				},
			},
			args: args{
				msg: []byte(`{"a":true}`),
				key: "known",
				handler: func(t *testing.T) Handler {
					return func(ctx context.Context, event ConsumableEvent[any]) error {
						assert.IsType(t, &TestMessage{}, event.Payload)
						return fmt.Errorf("handler-error")
					}
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "handler-error")
			},
		},
		{
			name: "success",
			fields: fields{
				keyToType: map[string]reflect.Type{
					"known": reflect.TypeOf(TestMessage{}),
				},
			},
			args: args{
				msg: []byte(`{"a":true}`),
				key: "known",
				handler: func(t *testing.T) Handler {
					return func(ctx context.Context, event ConsumableEvent[any]) error {
						assert.IsType(t, &TestMessage{}, event.Payload)
						return nil
					}
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := injectRoutingKeyToTypeContext(context.TODO(), tt.fields.keyToType)

			handler := TypeMappingHandler(tt.args.handler(t))
			err := handler(ctx, ConsumableEvent[json.RawMessage]{
				Payload:      tt.args.msg,
				DeliveryInfo: DeliveryInfo{RoutingKey: tt.args.key},
			})
			if !tt.wantErr(t, err) {
				return
			}
		})
	}
}

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

type (
	// Handler is the type definition for a function that is used to handle events that has been mapped with
	// RoutingKey <-> Type mappings from WithTypeMapping.
	// If processing fails, an error should be returned and the message will be re-queued
	Handler func(ctx context.Context, event ConsumableEvent[any]) error
	// EventHandler is the type definition for a function that is used to handle events of a specific type.
	// If processing fails, an error should be returned and the message will be re-queued
	EventHandler[T any] func(ctx context.Context, event ConsumableEvent[T]) error
	// RequestResponseEventHandler is the type definition for a function that is used to handle events of a specific
	// type and return a response with RequestResponseHandler.
	// If processing fails, an error should be returned and the message will be re-queued
	RequestResponseEventHandler[T any, R any] func(ctx context.Context, event ConsumableEvent[T]) (R, error)
)

func WithTypeMappingHandler(handler Handler) EventHandler[json.RawMessage] {
	return func(ctx context.Context, event ConsumableEvent[json.RawMessage]) error {
		message, exists := routingKeyToTypeFromContext(ctx, event)
		if !exists {
			return ErrNoMessageTypeForRouteKey
		}
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return err
		}
		msg := ConsumableEvent[any]{
			Metadata:     event.Metadata,
			DeliveryInfo: event.DeliveryInfo,
			Payload:      message,
		}
		return handler(ctx, msg)
	}
}

type routingKeyToTypeCtx string

const routingKeyToTypeCtxProperty routingKeyToTypeCtx = "routingKeyToType"

func injectRoutingKeyToTypeContext(ctx context.Context, keyToType RoutingKeyToType) context.Context {
	return context.WithValue(ctx, routingKeyToTypeCtxProperty, keyToType)
}

func routingKeyToTypeFromContext[T any](ctx context.Context, event ConsumableEvent[T]) (any, bool) {
	routingKey := event.DeliveryInfo.RoutingKey
	keyToType, ok := ctx.Value(routingKeyToTypeCtxProperty).(RoutingKeyToType)
	if !ok {
		return nil, false
	}

	typ, exists := keyToType[routingKey]
	if !exists {
		return nil, false
	}
	return reflect.New(typ).Interface(), true
}

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
		config := &QueueBindingConfig{
			routingKey:   routingKey,
			handler:      newWrappedHandler(handler),
			queueName:    queueName,
			exchangeName: exchangeName,
			kind:         kindTopic,
			transient:    true,
		}
		return c.messageHandlerBindQueueToExchange(config)
	}
}

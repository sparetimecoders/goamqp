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
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type queueConsumer struct {
	queue            string
	handlers         routingKeyHandler
	routingKeyToType routingKeyToType
	notificationCh   chan<- Notification
	errorCh          chan<- ErrorNotification
}

func (c *queueConsumer) consume(channel AmqpChannel, routingKeyToType routingKeyToType, notificationCh chan<- Notification, errorCh chan<- ErrorNotification) (<-chan amqp.Delivery, error) {
	c.routingKeyToType = routingKeyToType
	c.notificationCh = notificationCh
	c.errorCh = errorCh
	deliveries, err := channel.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return deliveries, nil
}

func (c *queueConsumer) loop(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		deliveryInfo := getDeliveryInfo(c.queue, delivery)
		eventReceived(c.queue, deliveryInfo.RoutingKey)

		// Establish which handler is invoked
		handler, ok := c.handlers.get(deliveryInfo.RoutingKey)
		if !ok {
			eventWithoutHandler(c.queue, deliveryInfo.RoutingKey)
			_ = delivery.Reject(false)
			continue
		}
		c.handleDelivery(handler, delivery, deliveryInfo)
	}
}

func (c *queueConsumer) handleDelivery(handler wrappedHandler, delivery amqp.Delivery, deliveryInfo DeliveryInfo) {
	tracingCtx := extractToContext(delivery.Headers)
	span := trace.SpanFromContext(tracingCtx)
	if !span.SpanContext().IsValid() {
		tracingCtx, span = otel.Tracer("amqp").Start(context.Background(), fmt.Sprintf("%s#%s", deliveryInfo.Queue, delivery.RoutingKey))
	}
	defer span.End()
	handlerCtx := injectRoutingKeyToTypeContext(tracingCtx, c.routingKeyToType)
	startTime := time.Now()

	uevt := unmarshalEvent{DeliveryInfo: deliveryInfo, Payload: delivery.Body}
	if err := handler(handlerCtx, uevt); err != nil {
		elapsed := time.Since(startTime).Milliseconds()
		notifyEventHandlerFailed(c.errorCh, deliveryInfo.RoutingKey, elapsed, err)
		if errors.Is(err, ErrParseJSON) {
			eventNotParsable(deliveryInfo.Queue, deliveryInfo.RoutingKey)
			_ = delivery.Nack(false, false)
		} else if errors.Is(err, ErrNoMessageTypeForRouteKey) {
			eventWithoutHandler(deliveryInfo.Queue, deliveryInfo.RoutingKey)
			_ = delivery.Reject(false)
		} else {
			eventNack(deliveryInfo.Queue, deliveryInfo.RoutingKey, elapsed)
			_ = delivery.Nack(false, true)
		}
		return
	}

	elapsed := time.Since(startTime).Milliseconds()
	notifyEventHandlerSucceed(c.notificationCh, deliveryInfo.RoutingKey, elapsed)
	_ = delivery.Ack(false)
	eventAck(deliveryInfo.Queue, deliveryInfo.RoutingKey, elapsed)
}

type queueConsumers map[string]*queueConsumer

func (c *queueConsumers) get(queueName, routingKey string) (wrappedHandler, bool) {
	consumerForQueue, ok := (*c)[queueName]
	if !ok {
		return nil, false
	}
	return consumerForQueue.handlers.get(routingKey)
}

func (c *queueConsumers) add(queueName, routingKey string, handler wrappedHandler) error {
	consumerForQueue, ok := (*c)[queueName]
	if !ok {
		consumerForQueue = &queueConsumer{
			queue:    queueName,
			handlers: make(routingKeyHandler),
		}
		(*c)[queueName] = consumerForQueue
	}
	if mappedRoutingKey, exists := consumerForQueue.handlers.exists(routingKey); exists {
		return fmt.Errorf("routingkey %s overlaps %s for queue %s, consider using AddQueueNameSuffix", routingKey, mappedRoutingKey, queueName)
	}
	consumerForQueue.handlers.add(routingKey, handler)
	return nil
}

func getDeliveryInfo(queueName string, delivery amqp.Delivery) DeliveryInfo {
	deliveryInfo := DeliveryInfo{
		Queue:      queueName,
		Exchange:   delivery.Exchange,
		RoutingKey: delivery.RoutingKey,
		Headers:    Headers(delivery.Headers),
	}
	return deliveryInfo
}

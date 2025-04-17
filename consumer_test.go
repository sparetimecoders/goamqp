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
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_Invalid_Payload(t *testing.T) {
	err := newWrappedHandler(func(ctx context.Context, event ConsumableEvent[string]) error {
		return nil
	})(context.TODO(), unmarshalEvent{Payload: []byte(`{"a":}`)})
	require.ErrorIs(t, err, ErrParseJSON)
	require.ErrorContains(t, err, "invalid character '}' looking for beginning of value")
}

func Test_Consume(t *testing.T) {
	consumer := queueConsumer{
		queue:    "aQueue",
		handlers: routingKeyHandler{},
	}
	channel := &MockAmqpChannel{consumeFn: func(queue, consumerName string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
		require.Equal(t, consumer.queue, queue)
		require.Equal(t, "", consumerName)
		require.False(t, autoAck)
		require.False(t, exclusive)
		require.False(t, noLocal)
		require.False(t, noWait)
		require.Nil(t, args)
		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{
			MessageId: "MESSAGE_ID",
		}
		close(deliveries)
		return deliveries, nil
	}}

	deliveries, err := consumer.consume(channel, nil, nil)
	require.NoError(t, err)
	delivery := <-deliveries
	require.Equal(t, "MESSAGE_ID", delivery.MessageId)
}

func Test_Consume_Failing(t *testing.T) {
	consumer := queueConsumer{
		queue:    "aQueue",
		handlers: routingKeyHandler{},
	}
	channel := &MockAmqpChannel{consumeFn: func(queue, consumerName string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
		return nil, fmt.Errorf("failed")
	}}

	_, err := consumer.consume(channel, nil, nil)
	require.EqualError(t, err, "failed")
}

func Test_ConsumerLoop(t *testing.T) {
	acker := MockAcknowledger{
		Acks:    make(chan Ack, 2),
		Nacks:   make(chan Nack, 1),
		Rejects: make(chan Reject, 1),
	}
	handler := newWrappedHandler(func(ctx context.Context, msg ConsumableEvent[Message]) error {
		if msg.Payload.Ok {
			return nil
		}
		return errors.New("failed")
	})

	consumer := queueConsumer{
		handlers: routingKeyHandler{},
		spanNameFn: func(info DeliveryInfo) string {
			return "span"
		},
	}
	consumer.handlers.add("key1", handler)
	consumer.handlers.add("key2", handler)

	queueDeliveries := make(chan amqp.Delivery, 4)

	queueDeliveries <- delivery(acker, "key1", true)
	queueDeliveries <- delivery(acker, "key2", true)
	queueDeliveries <- delivery(acker, "key2", false)
	queueDeliveries <- delivery(acker, "missing", true)
	close(queueDeliveries)

	consumer.loop(queueDeliveries)

	require.Len(t, acker.Rejects, 1)
	require.Len(t, acker.Nacks, 1)
	require.Len(t, acker.Acks, 2)
}

func Test_HandleDelivery(t *testing.T) {
	tests := []struct {
		name            string
		error           error
		numberOfAcks    int
		numberOfNacks   int
		numberOfRejects int
	}{
		{
			name:         "ok",
			numberOfAcks: 1,
		},
		{
			name:          "invalid JSON",
			error:         ErrParseJSON,
			numberOfNacks: 1,
		},
		{
			name:            "no match for routingkey",
			error:           ErrNoMessageTypeForRouteKey,
			numberOfRejects: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorNotifications := make(chan ErrorNotification, 1)
			notifications := make(chan Notification, 1)
			consumer := queueConsumer{
				errorCh:        errorNotifications,
				notificationCh: notifications,
				spanNameFn: func(info DeliveryInfo) string {
					return "span"
				},
			}
			deliveryInfo := DeliveryInfo{
				RoutingKey: "key",
			}
			acker := MockAcknowledger{
				Acks:    make(chan Ack, 1),
				Nacks:   make(chan Nack, 1),
				Rejects: make(chan Reject, 1),
			}
			handler := func(ctx context.Context, event unmarshalEvent) error {
				return tt.error
			}
			d := delivery(acker, "routingKey", true)
			consumer.handleDelivery(handler, d, deliveryInfo)
			if tt.error != nil {
				notification := <-errorNotifications
				require.EqualError(t, notification.Error, tt.error.Error())
			} else {
				notification := <-notifications
				require.Contains(t, notification.DeliveryInfo.RoutingKey, "key")
			}

			require.Len(t, acker.Acks, tt.numberOfAcks)
			require.Len(t, acker.Nacks, tt.numberOfNacks)
			require.Len(t, acker.Rejects, tt.numberOfRejects)
		})
	}
}

func delivery(acker MockAcknowledger, routingKey string, success bool) amqp.Delivery {
	body, _ := json.Marshal(Message{success})

	return amqp.Delivery{
		Body:         body,
		RoutingKey:   routingKey,
		Acknowledger: &acker,
	}
}

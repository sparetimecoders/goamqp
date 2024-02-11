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
	"reflect"
	"runtime"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_RequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	type response struct {
		Value string
	}
	expectedResponse := response{
		Value: "response",
	}
	err := RequestResponseHandler("key", func(ctx context.Context, msg ConsumableEvent[Message]) (response, error) {
		return expectedResponse, nil
	})(conn)
	require.NoError(t, err)

	require.Equal(t, 2, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: nil}, channel.ExchangeDeclarations[0])
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: nil}, channel.ExchangeDeclarations[1])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: nil}, channel.BindingDeclarations[0])

	require.Len(t, *conn.queueConsumers, 1)
	handler, _ := conn.queueConsumers.get("svc.direct.exchange.request.queue", "key")
	require.Equal(t, "github.com/sparetimecoders/goamqp.ServiceRequestConsumer[...].func1", runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name())
	missing, exists := conn.queueConsumers.get("miggins", "key")
	require.Nil(t, missing)
	require.False(t, exists)

	msg, _ := json.Marshal(Message{Ok: true})
	err = handler(context.TODO(), unmarshalEvent{
		Metadata: Metadata{},
		DeliveryInfo: DeliveryInfo{
			Headers: Headers{headerService: ""},
		},
		Payload: msg,
	})
	require.NoError(t, err)
	published := <-channel.Published
	var resp response
	require.NoError(t, json.Unmarshal(published.msg.Body, &resp))
	require.Equal(t, expectedResponse, resp)
}

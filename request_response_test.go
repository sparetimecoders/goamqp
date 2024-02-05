package goamqp

import (
	"context"
	"reflect"
	"runtime"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_RequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := RequestResponseHandler("key", func(ctx context.Context, msg ConsumableEvent[Message]) (response any, err error) {
		return nil, nil
	})(conn)
	require.NoError(t, err)

	require.Equal(t, 2, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[1])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(conn.queueHandlers.Queues()))

	handler, _ := conn.queueHandlers.Handlers("svc.direct.exchange.request.queue").get("key")
	require.Equal(t, "github.com/sparetimecoders/goamqp.ServiceRequestConsumer[...].func1", runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name())
}

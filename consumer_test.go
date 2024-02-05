package goamqp

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_EventStreamConsumer(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "events.topic.exchange.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_EventStreamConsumerWithOptFunc(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}, AddQueueNameSuffix("suffix")))
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc-suffix", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc-suffix", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "events.topic.exchange.queue.svc-suffix", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_EventStreamConsumerWithFailingOptFunc(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}, AddQueueNameSuffix("")))
	require.ErrorContains(t, err, "failed, empty queue suffix not allowed")
}

func Test_ServiceRequestConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceRequestConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}))

	require.NoError(t, err)
	require.Equal(t, 2, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[1])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "svc.direct.exchange.request.queue", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_ServiceRequestConsumer_ExchangeDeclareError(t *testing.T) {
	channel := NewMockAmqpChannel()
	declareError := errors.New("failed")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceRequestConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}))

	require.ErrorContains(t, err, "failed, failed to create exchange svc.headers.exchange.response, failed")
}

func Test_ServiceResponseConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceResponseConsumer("targetService", "key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}))

	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "targetService.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "targetService.headers.exchange.response.queue.svc", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "targetService.headers.exchange.response.queue.svc", noWait: false, exchange: "targetService.headers.exchange.response", key: "key", args: amqp.Table{headerService: "svc"}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "targetService.headers.exchange.response.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_TransientEventStreamConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return errors.New("failed")
	})(conn)

	require.NoError(t, err)
	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", key: "key", exchange: "events.topic.exchange", noWait: false, args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", kind: "topic", durable: true, autoDelete: false, internal: false, noWait: false, args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: false, autoDelete: true, noWait: false, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(conn.queueHandlers.Queues()))
	handler, ok := conn.queueHandlers.Handlers("events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f").get("key")
	require.True(t, ok)
	require.NotNil(t, handler)
}

func Test_TransientEventStreamConsumer_HandlerForRoutingKeyAlreadyExists(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	require.NoError(t, conn.queueHandlers.Add("events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", "root.key", nil))

	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("root.#", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return errors.New("failed")
	})(conn)

	require.EqualError(t, err, "routingkey root.# overlaps root.key for queue events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f, consider using AddQueueNameSuffix")
}

func Test_TransientEventStreamConsumer_ExchangeDeclareFails(t *testing.T) {
	channel := NewMockAmqpChannel()
	e := errors.New("failed")
	channel.ExchangeDeclarationError = &e

	testTransientEventStreamConsumerFailure(t, channel, e.Error())
}

func Test_TransientEventStreamConsumer_QueueDeclareFails(t *testing.T) {
	channel := NewMockAmqpChannel()
	e := errors.New("failed to create queue")
	channel.QueueDeclarationError = &e
	testTransientEventStreamConsumerFailure(t, channel, e.Error())
}

func testTransientEventStreamConsumerFailure(t *testing.T, channel *MockAmqpChannel, expectedError string) {
	conn := mockConnection(channel)

	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("key", func(ctx context.Context, msg ConsumableEvent[Message]) error {
		return errors.New("failed")
	})(conn)

	require.EqualError(t, err, expectedError)
}

func Test_ServiceResponseConsumer_ExchangeDeclareError(t *testing.T) {
	channel := NewMockAmqpChannel()
	declareError := errors.New("actual error message")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceResponseConsumer("targetService", "key", func(ctx context.Context, msg ConsumableEvent[TestMessage]) error {
		return nil
	}))

	require.ErrorContains(t, err, " failed, actual error message")
}

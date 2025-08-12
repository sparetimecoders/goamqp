// MIT License
//
// Copyright (c) 2023 sparetimecoders
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
	"reflect"
	"runtime"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UseLogger(t *testing.T) {
	conn := &Connection{errorLog: nil}
	logger := noOpLogger
	loggerName := runtime.FuncForPC(reflect.ValueOf(logger).Pointer()).Name()
	require.NoError(t, UseLogger(logger)(conn))
	setLoggerName := runtime.FuncForPC(reflect.ValueOf(conn.errorLog).Pointer()).Name()
	require.Equal(t, loggerName, setLoggerName)

	conn = &Connection{errorLog: nil}
	require.EqualError(t, UseLogger(nil)(conn), "cannot use nil as logger func")
	require.Nil(t, conn.errorLog)
}

func Test_CloseListener(t *testing.T) {
	listener := make(chan error)
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := CloseListener(listener)(conn)
	require.NoError(t, err)
	require.Equal(t, true, channel.NotifyCloseCalled)
	// nil is ignored
	channel.ForceClose(nil)
	channel.ForceClose(&amqp.Error{Code: 123, Reason: "Close reason"})
	err = <-listener
	require.EqualError(t, err, "Exception (123) Reason: \"Close reason\"")
}

func Test_EventStreamPublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.typeToKey[reflect.TypeOf(TestMessage{})] = "key"
	conn.typeToKey[reflect.TypeOf(TestMessage2{})] = "key2"
	p := NewPublisher()
	err := EventStreamPublisher(p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)

	published := <-channel.Published
	require.Equal(t, "key", published.key)

	err = p.PublishWithContext(context.Background(), TestMessage{Msg: "test"}, Header{"x-header", "header"})
	require.NoError(t, err)
	published = <-channel.Published

	require.Equal(t, 2, len(published.msg.Headers))
	require.Equal(t, "svc", published.msg.Headers["service"])
	require.Equal(t, "header", published.msg.Headers["x-header"])
}

func Test_QueuePublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.typeToKey[reflect.TypeOf(TestMessage{})] = "key"
	conn.typeToKey[reflect.TypeOf(TestMessage2{})] = "key2"
	p := NewPublisher()
	err := QueuePublisher(p, "destQueue")(conn)
	require.NoError(t, err)

	require.Equal(t, 0, len(channel.ExchangeDeclarations))

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)

	published := <-channel.Published
	require.Equal(t, "key", published.key)

	err = p.PublishWithContext(context.Background(), TestMessage{Msg: "test"}, Header{"x-header", "header"})
	require.NoError(t, err)
	published = <-channel.Published

	require.Equal(t, 3, len(published.msg.Headers))
	require.Equal(t, "svc", published.msg.Headers["service"])
	require.Equal(t, "header", published.msg.Headers["x-header"])
	require.Equal(t, "destQueue", published.msg.Headers["CC"].([]any)[0])
}

func Test_EventStreamPublisher_FailedToCreateExchange(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher()

	e := errors.New("failed to create exchange")
	channel.ExchangeDeclarationError = &e
	err := EventStreamPublisher(p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, "failed to declare exchange events.topic.exchange: failed to create exchange")
}

func Test_UseMessageLogger(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.typeToKey[reflect.TypeOf(TestMessage{})] = "routingkey"
	logger := &MockLogger{}
	p := NewPublisher()

	_ = conn.Start(context.Background(),
		UseMessageLogger(logger.logger()),
		ServicePublisher("service", p),
	)
	require.NotNil(t, conn.messageLogger)

	err := p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)
	<-channel.Published

	require.Equal(t, true, logger.outgoing)
	require.Equal(t, "routingkey", logger.routingKey)
	require.Equal(t, reflect.TypeOf(TestMessage{}), logger.eventType)
}

func Test_UseMessageLogger_Nil(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher()

	err := conn.Start(context.Background(),
		UseMessageLogger(nil),
		ServicePublisher("service", p),
	)
	require.Error(t, err)
}

func Test_UseMessageLogger_Default(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher()

	err := conn.Start(context.Background(),
		ServicePublisher("service", p),
	)
	require.NoError(t, err)
	require.NotNil(t, conn.messageLogger)
}

func Test_EventStreamConsumer(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))
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
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}, AddQueueNameSuffix("suffix")))
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
	err := conn.Start(context.Background(), EventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}, AddQueueNameSuffix("")))
	require.ErrorContains(t, err, "failed, empty queue suffix not allowed")
}

func Test_ServiceRequestConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceRequestConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))

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
	err := conn.Start(context.Background(), ServiceRequestConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))

	require.ErrorContains(t, err, "failed, failed to create exchange svc.headers.exchange.response: failed")
}

func Test_ServiceResponseConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceResponseConsumer("targetService", "key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))

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

func Test_ServiceResponseConsumer_ExchangeDeclareError(t *testing.T) {
	channel := NewMockAmqpChannel()
	declareError := errors.New("actual error message")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceResponseConsumer("targetService", "key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))

	require.ErrorContains(t, err, " failed, actual error message")
}

func Test_RequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := RequestResponseHandler("key", func(msg any, headers Headers) (response any, err error) {
		return nil, nil
	}, Message{})(conn)
	require.NoError(t, err)

	require.Equal(t, 2, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[1])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(conn.queueHandlers.Queues()))

	invoker, _ := conn.queueHandlers.Handlers("svc.direct.exchange.request.queue").Get("key")
	require.Equal(t, reflect.TypeOf(Message{}), invoker.eventType)
	require.Equal(t, "github.com/sparetimecoders/goamqp.Test_RequestResponseHandler.Test_RequestResponseHandler.RequestResponseHandler.func2.responseWrapper.func4", runtime.FuncForPC(reflect.ValueOf(invoker.msgHandler).Pointer()).Name())
}

func Test_ServicePublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.typeToKey[reflect.TypeOf(TestMessage{})] = "key"
	p := NewPublisher()

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)
	published := <-channel.Published
	require.Equal(t, "key", published.key)
	require.Equal(t, "svc.direct.exchange.request", published.exchange)
}

func Test_ServicePublisher_Multiple(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.typeToKey[reflect.TypeOf(TestMessage{})] = "key"
	conn.typeToKey[reflect.TypeOf(TestMessage2{})] = "key2"
	p := NewPublisher()

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)
	err = p.PublishWithContext(context.Background(), TestMessage2{Msg: "msg"})
	require.NoError(t, err)
	err = p.PublishWithContext(context.Background(), TestMessage{"test2", false})
	require.NoError(t, err)
	published := <-channel.Published
	require.Equal(t, "key", published.key)
	require.Equal(t, "svc.direct.exchange.request", published.exchange)
	published = <-channel.Published
	require.Equal(t, "key2", published.key)
	require.Equal(t, "svc.direct.exchange.request", published.exchange)
	published = <-channel.Published
	require.Equal(t, "key", published.key)
	require.Equal(t, "{\"Msg\":\"test2\",\"Success\":false}", string(published.msg.Body))
}

func Test_ServicePublisher_NoMatchingRoute(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher()

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), &TestMessage{Msg: "test"})
	require.True(t, errors.Is(err, ErrNoRouteForMessageType))
	require.EqualError(t, err, "no routingkey configured for message of type *goamqp.TestMessage")
}

func Test_ServicePublisher_ExchangeDeclareFail(t *testing.T) {
	e := errors.New("failed")
	channel := NewMockAmqpChannel()
	channel.ExchangeDeclarationError = &e
	conn := mockConnection(channel)

	p := NewPublisher()

	err := ServicePublisher("svc", p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, e.Error())
}

func Test_TransientEventStreamConsumer_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, errors.New("failed")
	}, Message{})(conn)

	require.NoError(t, err)
	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", key: "key", exchange: "events.topic.exchange", noWait: false, args: amqp.Table{}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", kind: "topic", durable: true, autoDelete: false, internal: false, noWait: false, args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: false, autoDelete: true, exclusive: true, noWait: false, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(conn.queueHandlers.Queues()))
	invoker, _ := conn.queueHandlers.Handlers("events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f").Get("key")
	require.Equal(t, reflect.TypeOf(Message{}), invoker.eventType)
}

func Test_TransientEventStreamConsumer_HandlerForRoutingKeyAlreadyExists(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	require.NoError(t, conn.queueHandlers.Add("events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", "root.key", &messageHandlerInvoker{}))

	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("root.#", func(i any, headers Headers) (any, error) {
		return nil, errors.New("failed")
	}, Message{})(conn)

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
	err := TransientEventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, errors.New("failed")
	}, Message{})(conn)

	require.EqualError(t, err, expectedError)
}

func Test_PublishNotify(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	notifier := make(chan amqp.Confirmation)
	err := PublishNotify(notifier)(conn)
	require.NoError(t, err)
	require.Equal(t, &notifier, channel.Confirms)
	require.Equal(t, true, channel.ConfirmCalled)
}

func Test_WithTypeMapping_KeyAlreadyExist(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := WithTypeMapping("key", TestMessage{})(conn)
	assert.NoError(t, err)
	err = WithTypeMapping("key", TestMessage2{})(conn)
	assert.EqualError(t, err, "mapping for routing key 'key' already registered to type 'goamqp.TestMessage'")
}

func Test_WithTypeMapping_TypeAlreadyExist(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := WithTypeMapping("key", TestMessage{})(conn)
	assert.NoError(t, err)
	err = WithTypeMapping("other", TestMessage{})(conn)
	assert.EqualError(t, err, "mapping for type 'goamqp.TestMessage' already registered to routing key 'key'")
}

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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_AmqpVersion(t *testing.T) {
	require.Equal(t, "_unknown_", amqpVersion())
}

func Test_UseLogger(t *testing.T) {
	conn := &Connection{errorLogF: nil}
	logger := noOpLogger
	loggerName := runtime.FuncForPC(reflect.ValueOf(logger).Pointer()).Name()
	require.NoError(t, UseLogger(logger)(conn))
	setLoggerName := runtime.FuncForPC(reflect.ValueOf(conn.errorLogF).Pointer()).Name()
	require.Equal(t, loggerName, setLoggerName)

	conn = &Connection{errorLogF: nil}
	require.EqualError(t, UseLogger(nil)(conn), "cannot use nil as logger func")
	require.Nil(t, conn.errorLogF)
}

func Test_getEventType(t *testing.T) {
	e, err := getEventType(TestMessage{})
	require.NoError(t, err)
	require.Equal(t, reflect.TypeOf(TestMessage{}), e)

	_, err = getEventType(reflect.TypeOf(TestMessage{}))
	require.Error(t, err)
}

func Test_Start_MultipleCallsFails(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		qosFn: func(prefetchCount, prefetchSize int, global bool) error {
			require.Equal(t, 20, prefetchCount)
			return nil
		},
	}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
	}
	err := conn.Start(context.Background())
	require.NoError(t, err)
	err = conn.Start(context.Background())
	require.Error(t, err)
	require.EqualError(t, err, "already started")
}

func Test_Start_SettingDefaultQosFails(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		qosFn: func(prefetchCount, prefetchSize int, global bool) error {
			return errors.New("error setting qos")
		},
	}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
	}
	err := conn.Start(context.Background())
	require.Error(t, err)
	require.EqualError(t, err, "error setting qos")
}

func Test_Start_SetupFails(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		consumeFn: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return nil, errors.New("error consuming queue")
		},
	}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
		handlers:    make(map[queueRoutingKey]messageHandlerInvoker),
	}
	err := conn.Start(context.Background(),
		EventStreamConsumer("test", func(i any, headers Headers) (any, error) {
			return nil, errors.New("failed")
		}, Message{}))
	require.Error(t, err)
	require.EqualError(t, err, "failed to create consumer for queue events.topic.exchange.queue.test. error consuming queue")
}

func Test_Start_WithPrefetchLimit_Resets_Qos(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		qosFn: func(cc int) func(prefetchCount, prefetchSize int, global bool) error {
			return func(prefetchCount, prefetchSize int, global bool) error {
				defer func() {
					cc++
				}()
				if cc == 0 {
					require.Equal(t, 20, prefetchCount)
				} else {
					require.Equal(t, 1, prefetchCount)
				}
				return nil
			}
		}(0),
	}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
	}
	err := conn.Start(context.Background(),
		WithPrefetchLimit(1),
	)
	require.NoError(t, err)
}

func Test_Start_ConnectionFail(t *testing.T) {
	orgDial := dialAmqp
	defer func() { dialAmqp = orgDial }()
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return nil, errors.New("failed to connect")
	}
	conn, err := NewFromURL("", "amqp://user:password@localhost:67333/a")
	require.NoError(t, err)
	err = conn.Start(context.Background())
	require.Error(t, err)
	require.EqualError(t, err, "failed to connect")
}

func TestMust(t *testing.T) {
	conn := Must(NewFromURL("", "amqp://user:password@localhost:67333/a"))
	require.NotNil(t, conn)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	_ = Must(NewFromURL("", "invalid"))
}

func Test_CloseCallsUnderlyingCloseMethod(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.started = true
	err := conn.Close()
	require.NoError(t, err)
	require.Equal(t, true, conn.connection.(*MockAmqpConnection).CloseCalled)
}

func Test_CloseWhenNotStarted(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	conn.started = false
	err := conn.Close()
	require.NoError(t, err)
	require.Equal(t, false, conn.connection.(*MockAmqpConnection).CloseCalled)
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

func Test_ConnectToAmqpUrl_Ok(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	orgDial := dialAmqp
	defer func() { dialAmqp = orgDial }()
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return mockAmqpConnection, nil
	}
	conn, err := NewFromURL("", "amqp://user:password@localhost:12345/vhost")
	require.NoError(t, err)
	err = conn.connectToAmqpURL()
	require.NoError(t, err)
	require.Equal(t, mockAmqpConnection, conn.connection)
	require.NotNil(t, conn.channel)
}

func Test_ConnectToAmqpUrl_ConnectionFailed(t *testing.T) {
	orgDial := dialAmqp
	defer func() { dialAmqp = orgDial }()
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return nil, errors.New("failure to connect")
	}
	conn := Connection{}
	err := conn.connectToAmqpURL()
	require.Error(t, err)
	require.Nil(t, conn.connection)
	require.Nil(t, conn.channel)
}

func Test_ConnectToAmqpUrl_FailToGetChannel(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{}
	orgDial := dialAmqp
	defer func() { dialAmqp = orgDial }()
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return mockAmqpConnection, nil
	}
	conn := Connection{}
	err := conn.connectToAmqpURL()
	require.Error(t, err)
	require.Nil(t, conn.connection)
	require.Nil(t, conn.channel)
}

func Test_FailingSetupFunc(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), func(c *Connection) error { return nil }, func(c *Connection) error { return fmt.Errorf("error message") })
	require.EqualError(t, err, "setup function <github.com/sparetimecoders/goamqp.Test_FailingSetupFunc.func2> failed, error message")
}

func Test_NewFromURL_ValidURL(t *testing.T) {
	c, err := NewFromURL("test", "amqp://user:password@localhost:5672/")
	require.NotNil(t, c)
	require.NoError(t, err)
}

func Test_AmqpConfig(t *testing.T) {
	require.Equal(t, fmt.Sprintf("servicename#_unknown_#@%s", hostName()), amqpConfig("servicename").Properties["connection_name"])
}

func Test_QueueDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := queueDeclare(channel, "test")
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "test", durable: true, autoDelete: false, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_TransientQueueDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := transientQueueDeclare(channel, "test")
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "test", durable: false, autoDelete: true, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_ExchangeDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()

	conn := mockConnection(channel)

	err := conn.exchangeDeclare(channel, "name", "topic")
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "name", kind: "topic", durable: true, autoDelete: false, noWait: false, args: amqp.Table{}}, channel.ExchangeDeclarations[0])
}

func Test_Consume(t *testing.T) {
	channel := NewMockAmqpChannel()
	_, err := consume(channel, "q")
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "q",
		consumer: "", autoAck: false, exclusive: false, noLocal: false, noWait: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_Publish(t *testing.T) {
	channel := NewMockAmqpChannel()
	headers := amqp.Table{}
	headers["key"] = "value"
	c := Connection{
		channel:       channel,
		messageLogger: noOpMessageLogger(),
	}
	err := c.publishMessage(context.Background(), Message{true}, "key", "exchange", headers)
	require.NoError(t, err)

	publish := <-channel.Published
	require.Equal(t, "key", publish.key)
	require.Equal(t, "exchange", publish.exchange)
	require.Equal(t, false, publish.immediate)
	require.Equal(t, false, publish.mandatory)

	msg := publish.msg
	require.Equal(t, "", msg.Type)
	require.Equal(t, "application/json", msg.ContentType)
	require.Equal(t, "", msg.AppId)
	require.Equal(t, "", msg.ContentEncoding)
	require.Equal(t, "", msg.CorrelationId)
	require.Equal(t, uint8(2), msg.DeliveryMode)
	require.Equal(t, "", msg.Expiration)
	require.Equal(t, "value", msg.Headers["key"])
	require.Equal(t, "", msg.ReplyTo)

	body := &Message{}
	_ = json.Unmarshal(msg.Body, &body)
	require.Equal(t, &Message{true}, body)
	require.Equal(t, "", msg.UserId)
	require.Equal(t, uint8(0), msg.Priority)
	require.Equal(t, "", msg.MessageId)
}
func Test_Publish_Marshal_Errir(t *testing.T) {
	channel := NewMockAmqpChannel()
	headers := amqp.Table{}
	headers["key"] = "value"
	c := Connection{
		channel:       channel,
		messageLogger: noOpMessageLogger(),
	}
	err := c.publishMessage(context.Background(), math.Inf(1), "key", "exchange", headers)
	require.EqualError(t, err, "json: unsupported value: +Inf")
}
func TestResponseWrapper(t *testing.T) {
	tests := []struct {
		name         string
		handlerResp  any
		handlerErr   error
		published    any
		publisherErr error
		wantErr      error
		wantResp     any
		headers      *Headers
	}{
		{
			name: "handler ok - no resp - nothing published",
		},
		{
			name:        "handler ok - with resp - published",
			handlerResp: Message{},
			published:   Message{},
			wantResp:    Message{},
		},
		{
			name:         "handler ok - with resp - publish error",
			handlerResp:  Message{},
			publisherErr: errors.New("amqp error"),
			wantErr:      errors.New("failed to publish response: amqp error"),
		},
		{
			name:       "handler error - no resp - nothing published",
			handlerErr: errors.New("failed"),
			wantErr:    errors.New("failed to process message: failed"),
		},
		{
			name:        "handler error - with resp - nothing published",
			handlerResp: Message{},
			handlerErr:  errors.New("failed"),
			wantErr:     errors.New("failed to process message: failed"),
		},
		{
			name:        "handler ok - with resp - missing header",
			handlerResp: Message{},
			headers:     &Headers{},
			wantErr:     errors.New("failed to extract service name: no service found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &mockPublisher{
				err:       tt.publisherErr,
				published: nil,
			}
			headers := Headers(map[string]any{headerService: "test"})

			if tt.headers != nil {
				headers = *tt.headers
			}
			resp, err := responseWrapper(func(i any, headers Headers) (any, error) {
				return tt.handlerResp, tt.handlerErr
			}, "key", p.publish)(&Message{}, headers)
			p.checkPublished(t, tt.published)

			require.Equal(t, tt.wantResp, resp)
			if tt.wantErr != nil {
				require.EqualError(t, tt.wantErr, err.Error())
			}
		})
	}

}

func Test_Headers(t *testing.T) {
	h := Headers{}
	require.NoError(t, h.validate())

	h = headers(amqp.Table{"valid": ""})
	require.NoError(t, h.validate())
	require.Equal(t, "", h.Get("valid"))
	require.Nil(t, h.Get("invalid"))

	h = headers(amqp.Table{"valid1": "1", "valid2": "2"})
	require.Equal(t, "1", h.Get("valid1"))
	require.Equal(t, "2", h.Get("valid2"))

	h = map[string]any{headerService: "p"}
	require.EqualError(t, h.validate(), "reserved key service used, please change to use another one")

	h = map[string]any{"": "p"}
	require.EqualError(t, h.validate(), "empty key not allowed")

	h = headers(amqp.Table{headerService: "peter"})
	require.Equal(t, h.Get(headerService), "peter")
}

func Test_DivertToMessageHandler(t *testing.T) {
	acker := MockAcknowledger{
		Acks:    make(chan Ack, 4),
		Nacks:   make(chan Nack, 1),
		Rejects: make(chan Reject, 1),
	}
	channel := MockAmqpChannel{Published: make(chan Publish, 1)}

	handlers := make(map[string]messageHandlerInvoker)
	msgInvoker := messageHandlerInvoker{
		eventType: reflect.TypeOf(Message{}),
		msgHandler: func(i any, headers Headers) (any, error) {
			if i.(*Message).Ok {
				return nil, nil
			}
			return nil, errors.New("failed")
		},
	}
	handlers["key1"] = msgInvoker
	handlers["key2"] = msgInvoker

	queueDeliveries := make(chan amqp.Delivery, 6)

	queueDeliveries <- delivery(acker, "key1", true)
	queueDeliveries <- delivery(acker, "key2", true)
	queueDeliveries <- delivery(acker, "key2", false)
	queueDeliveries <- delivery(acker, "missing", true)
	close(queueDeliveries)

	c := Connection{
		started:       true,
		channel:       &channel,
		messageLogger: noOpMessageLogger(),
		errorLogF:     noOpLogger,
	}
	c.divertToMessageHandlers(queueDeliveries, handlers)

	require.Equal(t, 1, len(acker.Rejects))
	require.Equal(t, 1, len(acker.Nacks))
	require.Equal(t, 2, len(acker.Acks))
}
func Test_messageHandlerBindQueueToExchange(t *testing.T) {
	e := errors.New("failed to create queue")
	channel := &MockAmqpChannel{
		QueueDeclarationError: &e,
	}
	conn := mockConnection(channel)

	cfg := &QueueBindingConfig{
		routingKey:   "routingkey",
		handler:      nil,
		eventType:    nil,
		queueName:    "queue",
		exchangeName: "exchange",
		kind:         kindDirect,
		headers:      nil,
	}
	err := conn.messageHandlerBindQueueToExchange(cfg)
	require.EqualError(t, err, "failed to create queue")
}
func delivery(acker MockAcknowledger, routingKey string, success bool) amqp.Delivery {
	body, _ := json.Marshal(Message{success})

	return amqp.Delivery{
		Body:         body,
		RoutingKey:   routingKey,
		Acknowledger: &acker,
	}
}

func Test_HandleMessage_Ack_WhenHandled(t *testing.T) {
	require.Equal(t, Ack{tag: 0x0, multiple: false}, <-testHandleMessage("{}", true).Acks)
}

func Test_HandleMessage_Nack_WhenUnhandled(t *testing.T) {
	require.Equal(t, Nack{tag: 0x0, multiple: false, requeue: true}, <-testHandleMessage("{}", false).Nacks)
}

func Test_HandleMessage_Reject_IfParseFails(t *testing.T) {
	require.Equal(t, Reject{tag: 0x0, requeue: false}, <-testHandleMessage("", true).Rejects)
}

func testHandleMessage(json string, handle bool) MockAcknowledger {
	type Message struct{}
	acker := NewMockAcknowledger()
	delivery := amqp.Delivery{
		Body:         []byte(json),
		Acknowledger: &acker,
	}
	c := &Connection{
		messageLogger: noOpMessageLogger(),
		errorLogF:     noOpLogger,
	}
	c.handleMessage(delivery, func(i any, headers Headers) (any, error) {
		if handle {
			return nil, nil
		}
		return nil, errors.New("failed")
	}, reflect.TypeOf(Message{}),
		"routingkey")
	return acker
}

func Test_EventStreamPublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p, err := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage2{}, "key2"})
	require.NoError(t, err)
	err = EventStreamPublisher(p)(conn)
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
	p, err := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage2{}, "key2"})
	require.NoError(t, err)
	err = QueuePublisher(p, "destQueue")(conn)
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

func Test_Publisher_ReservedHeader(t *testing.T) {
	p, err := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage2{}, "key2"})
	require.NoError(t, err)
	err = p.PublishWithContext(context.Background(), TestMessage{Msg: "test"}, Header{"service", "header"})
	require.EqualError(t, err, "reserved key service used, please change to use another one")
}

func Test_EventStreamPublisher_FailedToCreateExchange(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p, err := NewPublisher(Route{TestMessage{}, "key"})
	require.NoError(t, err)

	e := errors.New("failed to create exchange")
	channel.ExchangeDeclarationError = &e
	err = EventStreamPublisher(p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, "failed to declare exchange events.topic.exchange: failed to create exchange")
}

func Test_UseMessageLogger(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	logger := &MockLogger{}
	p, err := NewPublisher(Route{TestMessage{}, "routingkey"})
	require.NoError(t, err)

	_ = conn.Start(context.Background(),
		UseMessageLogger(logger.logger()),
		ServicePublisher("service", p),
	)
	require.NotNil(t, conn.messageLogger)

	err = p.PublishWithContext(context.Background(), TestMessage{"test", true})
	require.NoError(t, err)
	<-channel.Published

	require.Equal(t, true, logger.outgoing)
	require.Equal(t, "routingkey", logger.routingKey)
	require.Equal(t, reflect.TypeOf(TestMessage{}), logger.eventType)
}

func Test_UseMessageLogger_Nil(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p, err := NewPublisher(Route{TestMessage{}, "routingkey"})
	require.NoError(t, err)

	err = conn.Start(context.Background(),
		UseMessageLogger(nil),
		ServicePublisher("service", p),
	)
	require.Error(t, err)
}

func Test_UseMessageLogger_Default(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p, err := NewPublisher(Route{TestMessage{}, "routingkey"})
	require.NoError(t, err)

	err = conn.Start(context.Background(),
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
	require.EqualError(t, err, "setup function <github.com/sparetimecoders/goamqp.EventStreamConsumer.func1> failed, queuebinding setup function <github.com/sparetimecoders/goamqp.AddQueueNameSuffix.func1> failed, empty queue suffix not allowed")
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

	require.EqualError(t, err, "setup function <github.com/sparetimecoders/goamqp.ServiceRequestConsumer.func1> failed, failed to create exchange svc.headers.exchange.response: failed")
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
	declareError := errors.New("failed")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(context.Background(), ServiceResponseConsumer("targetService", "key", func(i any, headers Headers) (any, error) {
		return nil, nil
	}, TestMessage{}))

	require.EqualError(t, err, "setup function <github.com/sparetimecoders/goamqp.ServiceResponseConsumer.func1> failed, failed")
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

	require.Equal(t, 1, len(conn.handlers))

	invoker := conn.handlers[queueRoutingKey{
		Queue:      "svc.direct.exchange.request.queue",
		RoutingKey: "key",
	}]
	require.Equal(t, "svc.direct.exchange.request.queue", invoker.Queue)
	require.Equal(t, "key", invoker.RoutingKey)
	require.Equal(t, reflect.TypeOf(Message{}), invoker.eventType)
	require.Equal(t, "github.com/sparetimecoders/goamqp.responseWrapper.func1", runtime.FuncForPC(reflect.ValueOf(invoker.msgHandler).Pointer()).Name())
}

func Test_ServicePublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p, err := NewPublisher(Route{TestMessage{}, "key"})
	require.NoError(t, err)

	err = ServicePublisher("svc", p)(conn)
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
	p, err := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage2{}, "key2"})
	require.NoError(t, err)

	err = ServicePublisher("svc", p)(conn)
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
	p, err := NewPublisher(Route{TestMessage2{}, "key"})
	require.NoError(t, err)

	err = ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.PublishWithContext(context.Background(), &TestMessage{Msg: "test"})
	require.EqualError(t, err, "no routingkey configured for message of type *goamqp.TestMessage")
}

func Test_ServicePublisher_ExchangeDeclareFail(t *testing.T) {
	e := errors.New("failed")
	channel := NewMockAmqpChannel()
	channel.ExchangeDeclarationError = &e
	conn := mockConnection(channel)

	p, err := NewPublisher(Route{TestMessage{}, "key"})
	require.NoError(t, err)

	err = ServicePublisher("svc", p)(conn)
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
	require.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: false, autoDelete: true, noWait: false, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(conn.handlers))
	key := queueRoutingKey{
		Queue:      "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f",
		RoutingKey: "key",
	}
	invoker := conn.handlers[key]
	require.Equal(t, reflect.TypeOf(Message{}), invoker.eventType)
	require.Equal(t, "key", invoker.RoutingKey)
	require.Equal(t, "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", invoker.Queue)
	require.Equal(t, key, invoker.queueRoutingKey)
}

func Test_TransientEventStreamConsumer_HandlerForRoutingKeyAlreadyExists(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	key := queueRoutingKey{
		Queue:      "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f",
		RoutingKey: "key",
	}
	conn.handlers[key] = messageHandlerInvoker{}

	uuid.SetRand(badRand{})
	err := TransientEventStreamConsumer("key", func(i any, headers Headers) (any, error) {
		return nil, errors.New("failed")
	}, Message{})(conn)

	require.EqualError(t, err, "routingkey key for queue events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f already assigned to handler for type %!s(<nil>), cannot assign goamqp.Message, consider using AddQueueNameSuffix")
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

func TestEmptyQueueNameSuffix(t *testing.T) {
	require.EqualError(t, AddQueueNameSuffix("")(&QueueBindingConfig{}), ErrEmptySuffix.Error())
}

func TestQueueNameSuffix(t *testing.T) {
	cfg := &QueueBindingConfig{queueName: "queue"}
	require.NoError(t, AddQueueNameSuffix("suffix")(cfg))
	require.Equal(t, "queue-suffix", cfg.queueName)
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

type Message struct {
	Ok bool
}

type mockPublisher struct {
	err       error
	published any
}

func (m *mockPublisher) publish(ctx context.Context, targetService, routingKey string, msg any) error {
	if m.err != nil {
		return m.err
	}
	m.published = msg
	return nil
}

func (m *mockPublisher) checkPublished(t *testing.T, i any) {
	require.EqualValues(t, m.published, i)
}

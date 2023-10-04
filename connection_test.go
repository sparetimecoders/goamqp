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
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	handlers2 "github.com/sparetimecoders/goamqp/internal/handlers"
)

func Test_AmqpVersion(t *testing.T) {
	require.Equal(t, "_unknown_", amqpVersion())
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
		serviceName:   "test",
		connection:    mockAmqpConnection,
		channel:       mockChannel,
		queueHandlers: &handlers2.QueueHandlers[messageHandlerInvoker]{},
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

func Test_Must(t *testing.T) {
	conn := Must(NewFromURL("", "amqp://user:password@localhost:67333/a"))
	require.NotNil(t, conn)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	_ = Must(NewFromURL("", "invalid"))
}

func Test_URI(t *testing.T) {
	conn := Must(NewFromURL("", "amqp://user:password@localhost:67333/a"))
	require.NotNil(t, conn)
	require.Equal(t, "localhost", conn.URI().Host)
	require.Equal(t, 67333, conn.URI().Port)
	require.Equal(t, "a", conn.URI().Vhost)
	require.Equal(t, "user", conn.URI().Username)
	require.Equal(t, "password", conn.URI().Password)
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

func Test_Publish_Marshal_Error(t *testing.T) {
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

func Test_DivertToMessageHandler(t *testing.T) {
	acker := MockAcknowledger{
		Acks:    make(chan Ack, 4),
		Nacks:   make(chan Nack, 1),
		Rejects: make(chan Reject, 1),
	}
	channel := MockAmqpChannel{Published: make(chan Publish, 1)}

	handlers := &handlers2.QueueHandlers[messageHandlerInvoker]{}
	msgInvoker := &messageHandlerInvoker{
		eventType: reflect.TypeOf(Message{}),
		msgHandler: func(i any, headers Headers) (any, error) {
			if i.(*Message).Ok {
				return nil, nil
			}
			return nil, errors.New("failed")
		},
	}
	require.NoError(t, handlers.Add("q", "key1", msgInvoker))
	require.NoError(t, handlers.Add("q", "key2", msgInvoker))

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
	c.divertToMessageHandlers(queueDeliveries, handlers.Queues()[0].Handlers)

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
	}, reflect.TypeOf(Message{}))
	return acker
}

func Test_Publisher_ReservedHeader(t *testing.T) {
	p := NewPublisher()
	err := p.PublishWithContext(context.Background(), TestMessage{Msg: "test"}, Header{"service", "header"})
	require.EqualError(t, err, "reserved key service used, please change to use another one")
}

func TestEmptyQueueNameSuffix(t *testing.T) {
	require.EqualError(t, AddQueueNameSuffix("")(&QueueBindingConfig{}), ErrEmptySuffix.Error())
}

func TestQueueNameSuffix(t *testing.T) {
	cfg := &QueueBindingConfig{queueName: "queue"}
	require.NoError(t, AddQueueNameSuffix("suffix")(cfg))
	require.Equal(t, "queue-suffix", cfg.queueName)
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

func TestConnection_TypeMappingHandler(t *testing.T) {
	type fields struct {
		typeToKey map[reflect.Type]string
		keyToType map[string]reflect.Type
	}
	type args struct {
		handler func(t *testing.T) HandlerFunc
		msg     json.RawMessage
		key     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    any
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "no mapped type",
			fields: fields{},
			args: args{
				msg: []byte(`{"a":true}`),
				key: "unknown",
				handler: func(t *testing.T) HandlerFunc {
					return func(msg any, headers Headers) (response any, err error) {
						return nil, nil
					}
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "no mapped type found for routing key 'unknown'")
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
				handler: func(t *testing.T) HandlerFunc {
					return func(msg any, headers Headers) (response any, err error) {
						return nil, nil
					}
				},
			},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "unexpected end of JSON input")
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
				handler: func(t *testing.T) HandlerFunc {
					return func(msg any, headers Headers) (response any, err error) {
						assert.IsType(t, &TestMessage{}, msg)
						return nil, fmt.Errorf("handler-error")
					}
				},
			},
			want: nil,
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
				handler: func(t *testing.T) HandlerFunc {
					return func(msg any, headers Headers) (response any, err error) {
						assert.IsType(t, &TestMessage{}, msg)
						return "OK", nil
					}
				},
			},
			want:    "OK",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connection{
				typeToKey: tt.fields.typeToKey,
				keyToType: tt.fields.keyToType,
			}

			handler := c.TypeMappingHandler(tt.args.handler(t))
			res, err := handler(&tt.args.msg, headers(make(amqp.Table), tt.args.key))
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equalf(t, tt.want, res, "TypeMappingHandler()")
		})
	}
}

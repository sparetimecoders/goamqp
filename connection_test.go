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
	"math"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_AmqpVersion(t *testing.T) {
	require.Equal(t, "_unknown_", version())
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
		serviceName:    "test",
		connection:     mockAmqpConnection,
		channel:        mockChannel,
		queueConsumers: &queueConsumers{},
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

func Test_messageHandlerBindQueueToExchange(t *testing.T) {
	tests := []struct {
		name                     string
		queueDeclarationError    error
		exchangeDeclarationError error
	}{
		{
			name: "ok",
		},
		{
			name:                  "queue declare error",
			queueDeclarationError: errors.New("failed to create queue"),
		},
		{
			name:                     "exchange declare error",
			exchangeDeclarationError: errors.New("failed to create exchange"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel := &MockAmqpChannel{
				QueueDeclarationError:    &tt.queueDeclarationError,
				ExchangeDeclarationError: &tt.exchangeDeclarationError,
			}
			conn := mockConnection(channel)
			cfg := &QueueBindingConfig{
				routingKey:   "routingkey",
				handler:      nil,
				queueName:    "queue",
				exchangeName: "exchange",
				kind:         kindDirect,
				headers:      nil,
			}
			err := conn.messageHandlerBindQueueToExchange(cfg)
			if tt.queueDeclarationError != nil {
				require.ErrorIs(t, err, tt.queueDeclarationError)
			} else if tt.exchangeDeclarationError != nil {
				require.ErrorIs(t, err, tt.exchangeDeclarationError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_Start_SetupFails(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		consumeFn: func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return nil, errors.New("error consuming queue")
		},
	}
	conn := &Connection{
		serviceName:    "test",
		connection:     mockAmqpConnection,
		channel:        mockChannel,
		queueConsumers: &queueConsumers{},
	}
	err := conn.Start(context.Background(),
		EventStreamConsumer("test", func(ctx context.Context, msg ConsumableEvent[Message]) error {
			return errors.New("failed")
		}))
	require.Error(t, err)
	require.EqualError(t, err, "failed to create consumer for queue events.topic.exchange.queue.test. error consuming queue")
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
	err := queueDeclare(channel, "test", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "test", durable: true, autoDelete: false, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_TransientQueueDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := queueDeclare(channel, "test", true)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "test", durable: false, autoDelete: true, exclusive: true, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_ExchangeDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()

	err := exchangeDeclare(channel, "name", "topic")
	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "name", kind: "topic", durable: true, autoDelete: false, noWait: false, args: nil}, channel.ExchangeDeclarations[0])
}

func Test_Publish_Fail(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := publishMessage(context.Background(), channel, Message{true}, "failed", "exchange", nil)
	require.EqualError(t, err, "failed")
}

func Test_Publish(t *testing.T) {
	channel := NewMockAmqpChannel()
	headers := amqp.Table{}
	headers["key"] = "value"
	err := publishMessage(context.Background(), channel, Message{true}, "key", "exchange", headers)
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
	err := publishMessage(context.Background(), channel, math.Inf(1), "key", "exchange", headers)
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
			wantErr:      errors.New("failed to publish response, amqp error"),
		},
		{
			name:       "handler error - no resp - nothing published",
			handlerErr: errors.New("failed"),
			wantErr:    errors.New("failed to process message, failed"),
		},
		{
			name:        "handler error - with resp - nothing published",
			handlerResp: Message{},
			handlerErr:  errors.New("failed"),
			wantErr:     errors.New("failed to process message, failed"),
		},
		{
			name:        "handler ok - with resp - missing header",
			handlerResp: Message{},
			headers:     &Headers{},
			wantErr:     errors.New("failed to extract service name, no service found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &mockPublisher[any]{
				err:       tt.publisherErr,
				published: nil,
			}
			headers := Headers(map[string]any{headerService: "test"})

			if tt.headers != nil {
				headers = *tt.headers
			}
			err := responseWrapper(func(ctx context.Context, event ConsumableEvent[Message]) (any, error) {
				return tt.handlerResp, tt.handlerErr
			}, "key", p.publish)(context.TODO(), ConsumableEvent[Message]{
				DeliveryInfo: DeliveryInfo{Headers: headers},
			})
			p.checkPublished(t, tt.published)

			// require.Equal(t, tt.wantResp, resp)
			if tt.wantErr != nil {
				require.EqualError(t, tt.wantErr, err.Error())
			}
		})
	}
}

func Test_Publisher_ReservedHeader(t *testing.T) {
	p := NewPublisher()
	err := p.Publish(context.Background(), TestMessage{Msg: "test"}, Header{"service", "header"})
	require.EqualError(t, err, "reserved key service used, please change to use another one")
}

type Message struct {
	Ok bool
}

type mockPublisher[R any] struct {
	err       error
	published R
}

func (m *mockPublisher[R]) publish(ctx context.Context, targetService, routingKey string, msg R) error {
	if m.err != nil {
		return m.err
	}
	m.published = msg
	return nil
}

func (m *mockPublisher[R]) checkPublished(t *testing.T, i R) {
	require.EqualValues(t, m.published, i)
}

package goamqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"runtime"
	"testing"
)

func Test_AmqpVersion(t *testing.T) {
	require.Equal(t, "_unknown_", amqpVersion())
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
	err := conn.Start()
	require.NoError(t, err)
	err = conn.Start()
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
	err := conn.Start()
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
	err := conn.Start(
		EventStreamListener("test", func(i interface{}, headers Headers) (interface{}, error) {
			return nil, errors.New("failed")
		}, reflect.TypeOf(Message{})))
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
	err := conn.Start(
		WithPrefetchLimit(1),
	)
	require.NoError(t, err)
}

func Test_Start_ConnectionFail(t *testing.T) {
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return nil, errors.New("failed to connect")
	}
	conn, err := NewFromURL("", "amqp://user:password@localhost:67333/a")
	require.NoError(t, err)
	err = conn.Start()
	require.Error(t, err)
	require.EqualError(t, err, "failed to connect")
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
}

func Test_ConnectToAmqpUrl_Ok(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return mockAmqpConnection, nil
	}
	conn := Connection{config: AmqpConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     12345,
		VHost:    "vhost",
	}}
	err := conn.connectToAmqpURL()
	require.NoError(t, err)
	require.Equal(t, mockAmqpConnection, conn.connection)
	require.NotNil(t, conn.channel)
}

func Test_ConnectToAmqpUrl_ConnectionFailed(t *testing.T) {
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
	err := conn.Start(func(c *Connection) error { return nil }, func(c *Connection) error { return fmt.Errorf("error message") })
	require.EqualError(t, err, "setup function <gitlab.com/sparetimecoders/goamqp.Test_FailingSetupFunc.func2> failed, error message")
}

func Test_NewFromURL_InvalidURL(t *testing.T) {
	c, err := NewFromURL("test", "amqp://")
	require.Nil(t, c)
	require.EqualError(t, err, "connection url is invalid, amqp://")
}

func Test_NewFromURL_ValidURL(t *testing.T) {
	c, err := NewFromURL("test", "amqp://user:password@localhost:5672/")
	require.NotNil(t, c)
	require.NoError(t, err)
}

func Test_NewFromConfig(t *testing.T) {
	config, err := ParseAmqpURL("amqp://user:password@localhost:5672/")
	require.NoError(t, err)
	c := New("test", config)
	require.NotNil(t, c)
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
		messageLogger: NoOpMessageLogger(),
	}
	err := c.publishMessage(Message{true}, "key", "exchange", headers)
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
		messageLogger: NoOpMessageLogger(),
	}
	err := c.publishMessage(math.Inf(1), "key", "exchange", headers)
	require.EqualError(t, err, "json: unsupported value: +Inf")
}
func TestResponseWrapper(t *testing.T) {
	tests := []struct {
		name         string
		handlerResp  interface{}
		handlerErr   error
		published    interface{}
		publisherErr error
		wantErr      error
		wantResp     interface{}
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
			headers := Headers(map[string]interface{}{headerService: "test"})

			if tt.headers != nil {
				headers = *tt.headers
			}
			resp, err := ResponseWrapper(func(i interface{}, headers Headers) (interface{}, error) {
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

	h = map[string]interface{}{headerService: "p"}
	require.EqualError(t, h.validate(), "reserved key service used, please change to use another one")

	h = map[string]interface{}{"": "p"}
	require.EqualError(t, h.validate(), "empty key not allowed")

	h = headers(amqp.Table{headerService: "peter"})
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
		msgHandler: func(i interface{}, headers Headers) (interface{}, error) {
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
		messageLogger: NoOpMessageLogger(),
		log:           &noOpLogger{},
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

	err := conn.messageHandlerBindQueueToExchange("queue", "exchange", "routingkey", kindDirect, nil, nil, nil)
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
		messageLogger: NoOpMessageLogger(),
		log:           &noOpLogger{},
	}
	c.handleMessage(delivery, func(i interface{}, headers Headers) (interface{}, error) {
		if handle {
			return nil, nil
		}
		return nil, errors.New("failed")
	}, reflect.TypeOf(Message{}),
		"routingkey")
	return acker
}

//func Test_HandleRequestResponse_Reject_IfParseFails(t *testing.T) {
//	_, acker := testHandleRequestResponse("", true, false)
//	require.Equal(t, Reject{tag: 0x0, requeue: false}, <-acker.Rejects)
//}
//
//func Test_HandleRequestResponse_NackRequeuedWhenHandlingFailed(t *testing.T) {
//	_, acker := testHandleRequestResponse("{}", false, false)
//	require.Equal(t, Nack{tag: 0x0, multiple: false, requeue: true}, <-acker.Nacks)
//}
//
//func Test_HandleRequestResponse_NackNotRequeuedWhenPublishFails(t *testing.T) {
//	_, acker := testHandleRequestResponse("{}", true, true)
//	require.Equal(t, Nack{tag: 0x0, multiple: false, requeue: false}, <-acker.Nacks)
//}
//
//func Test_HandleRequestResponse_AckWhenSuccess(t *testing.T) {
//	channel, acker := testHandleRequestResponse("{}", true, false)
//	require.Equal(t, Ack{tag: 0x0, multiple: false}, <-acker.Acks)
//	require.Equal(t, Publish{exchange: "", key: "ok", mandatory: false, immediate: false, msg: amqp.Publishing{Headers: amqp.Table{"service": interface{}(nil)}, ContentType: "application/json", ContentEncoding: "", DeliveryMode: 0x2, Body: []uint8{0x7b, 0x22, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x22, 0x7d}}}, <-channel.Published)
//
//}
//
//func testHandleRequestResponse(json string, handled, publishFail bool) (MockAmqpChannel, MockAcknowledger) {
//	acker := NewMockAcknowledger()
//	channel := MockAmqpChannel{Published: make(chan Publish, 1)}
//	delivery := amqp.Delivery{
//		Body:         []byte(json),
//		Acknowledger: &acker,
//	}
//	invoker := messageHandlerInvoker{
//		eventType: reflect.TypeOf(Message{}),
//		ResponseHandler: func(i2 interface{}) (i interface{}, b bool) {
//			return Delayed{}, handled
//		},
//	}
//	if publishFail {
//		invoker.queueRoutingKey = queueRoutingKey{
//			RoutingKey: "failed",
//		}
//	} else {
//		invoker.queueRoutingKey = queueRoutingKey{
//			RoutingKey: "ok",
//		}
//	}
//	c := &Connection{
//		channel:       &channel,
//		messageLogger: NoOpMessageLogger(),
//	}
//
//	c.handleRequestResponse(delivery, invoker, "r")
//	return channel, acker
//}

func Test_EventStreamPublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage{}, "key"})
	err := EventStreamPublisher(p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, kind: "topic", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	require.NoError(t, err)

	published := <-channel.Published
	require.Equal(t, "key", published.key)

	err = p.Publish(TestMessage{Msg: "test"}, Header{"x-header", "header"})
	require.NoError(t, err)
	published = <-channel.Published

	require.Equal(t, 2, len(published.msg.Headers))
	require.Equal(t, "svc", published.msg.Headers["service"])
	require.Equal(t, "header", published.msg.Headers["x-header"])
}

func Test_Publisher_ReservedHeader(t *testing.T) {
	p := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage{}, "key"})
	err := p.Publish(TestMessage{Msg: "test"}, Header{"service", "header"})
	require.EqualError(t, err, "reserved key service used, please change to use another one")
}

func Test_EventStreamPublisher_FailedToCreateExchange(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"})
	e := errors.New("failed to create exchange")
	channel.ExchangeDeclarationError = &e
	err := EventStreamPublisher(p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, "failed to declare exchange events.topic.exchange: failed to create exchange")
}

func Test_UseMessageLogger(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	logger := &MockLogger{}
	p := NewPublisher(Route{TestMessage{}, "routingkey"})
	_ = conn.Start(
		UseMessageLogger(logger.logger()),
		ServicePublisher("service", p),
	)
	require.NotNil(t, conn.messageLogger)

	err := p.Publish(TestMessage{"test", true})
	require.NoError(t, err)
	<-channel.Published

	require.Equal(t, true, logger.outgoing)
	require.Equal(t, "routingkey", logger.routingKey)
	require.Equal(t, reflect.TypeOf(TestMessage{}), logger.eventType)
}

func Test_UseMessageLogger_Nil(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "routingkey"})
	err := conn.Start(
		UseMessageLogger(nil),
		ServicePublisher("service", p),
	)
	require.Error(t, err)
}

func Test_UseMessageLogger_Default(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "routingkey"})
	err := conn.Start(
		ServicePublisher("service", p),
	)
	require.NoError(t, err)
	require.NotNil(t, conn.messageLogger)
}

func Test_EventStreamListener(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(EventStreamListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, nil
	}, reflect.TypeOf(TestMessage{})))
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

func Test_ServiceRequestListener_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(ServiceRequestListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, nil
	}, reflect.TypeOf(TestMessage{})))

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

func Test_ServiceRequestListener_ExchangeDeclareError(t *testing.T) {
	channel := NewMockAmqpChannel()
	declareError := errors.New("failed")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(ServiceRequestListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, nil
	}, reflect.TypeOf(TestMessage{})))

	require.EqualError(t, err, "setup function <gitlab.com/sparetimecoders/goamqp.ServiceRequestListener.func1> failed, failed to create exchange svc.headers.exchange.response: failed")
}

func Test_ServiceResponseListener_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(ServiceResponseListener("targetService", "key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, nil
	}, reflect.TypeOf(TestMessage{})))

	require.NoError(t, err)
	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "targetService.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 1, len(channel.QueueDeclarations))
	require.Equal(t, QueueDeclaration{name: "svc.headers.exchange.response", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	require.Equal(t, 1, len(channel.BindingDeclarations))
	require.Equal(t, BindingDeclaration{queue: "svc.headers.exchange.response", noWait: false, exchange: "targetService.headers.exchange.response", key: "key", args: amqp.Table{headerService: "svc"}}, channel.BindingDeclarations[0])

	require.Equal(t, 1, len(channel.Consumers))
	require.Equal(t, Consumer{queue: "svc.headers.exchange.response", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_ServiceResponseListener_ExchangeDeclareError(t *testing.T) {
	channel := NewMockAmqpChannel()
	declareError := errors.New("failed")
	channel.ExchangeDeclarationError = &declareError
	conn := mockConnection(channel)
	err := conn.Start(ServiceResponseListener("targetService", "key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, nil
	}, reflect.TypeOf(TestMessage{})))

	require.EqualError(t, err, "setup function <gitlab.com/sparetimecoders/goamqp.ServiceResponseListener.func1> failed, failed")
}
func Test_RequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := RequestResponseHandler("key", func(msg interface{}, headers Headers) (response interface{}, err error) {
		return nil, nil
	}, reflect.TypeOf(Message{}))(conn)
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
	require.Equal(t, "gitlab.com/sparetimecoders/goamqp.ResponseWrapper.func1", runtime.FuncForPC(reflect.ValueOf(invoker.msgHandler).Pointer()).Name())
}

func Test_ServicePublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"})

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	require.NoError(t, err)
	published := <-channel.Published
	require.Equal(t, "key", published.key)
	require.Equal(t, "svc.direct.exchange.request", published.exchange)
}

func Test_ServicePublisher_Multiple(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"}, Route{TestMessage2{}, "key2"})

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	require.NoError(t, err)
	err = p.Publish(TestMessage2{Msg: "msg"})
	require.NoError(t, err)
	err = p.Publish(TestMessage{"test2", false})
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
	p := NewPublisher(Route{TestMessage2{}, "key"})

	err := ServicePublisher("svc", p)(conn)
	require.NoError(t, err)

	require.Equal(t, 1, len(channel.ExchangeDeclarations))
	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[0])

	require.Equal(t, 0, len(channel.QueueDeclarations))
	require.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(&TestMessage{Msg: "test"})
	require.EqualError(t, err, "no routingkey configured for message of type *goamqp.TestMessage")
}

func Test_ServicePublisher_ExchangeDeclareFail(t *testing.T) {
	e := errors.New("failed")
	channel := NewMockAmqpChannel()
	channel.ExchangeDeclarationError = &e
	conn := mockConnection(channel)

	p := NewPublisher(Route{TestMessage{}, "key"})
	err := ServicePublisher("svc", p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, e.Error())
}

//func Test_RequestResponseHandler(t *testing.T) {
//	channel := NewMockAmqpChannel()
//	conn := mockConnection(channel)
//	handlerErr := RequestResponseHandler("key", func(i interface{}) (interface{}, bool) {
//		return Message{}, true
//	}, reflect.TypeOf(Message{}))(conn)
//	require.NoError(t, handlerErr)
//
//	require.Equal(t, 2, len(channel.ExchangeDeclarations))
//	require.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, kind: "headers", args: amqp.Table{}}, channel.ExchangeDeclarations[0])
//	require.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, kind: "direct", args: amqp.Table{}}, channel.ExchangeDeclarations[1])
//
//	require.Equal(t, 1, len(channel.QueueDeclarations))
//	require.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])
//
//	require.Equal(t, 1, len(channel.BindingDeclarations))
//	require.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])
//}

func Test_TransientEventStreamListener_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	uuid.SetRand(badRand{})
	err := TransientEventStreamListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, errors.New("failed")
	}, reflect.TypeOf(Message{}))(conn)

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
	//require.Equal(t, "", invoker.ResponseExchange)
	require.Equal(t, key, invoker.queueRoutingKey)
}

func Test_TransientEventStreamListener_HandlerForRoutingKeyAlreadyExists(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	key := queueRoutingKey{
		Queue:      "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f",
		RoutingKey: "key",
	}
	conn.handlers[key] = messageHandlerInvoker{}

	uuid.SetRand(badRand{})
	err := TransientEventStreamListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, errors.New("failed")
	}, reflect.TypeOf(Message{}))(conn)

	require.EqualError(t, err, "routingkey key for queue events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f already assigned to handler for type %!s(<nil>), cannot assign goamqp.Message")
}

func Test_TransientEventStreamListener_ExchangeDeclareFails(t *testing.T) {
	channel := NewMockAmqpChannel()
	e := errors.New("failed")
	channel.ExchangeDeclarationError = &e

	testTransientEventStreamListenerFailure(t, channel, e.Error())
}

func Test_TransientEventStreamListener_QueueDeclareFails(t *testing.T) {
	channel := NewMockAmqpChannel()
	e := errors.New("failed to create queue")
	channel.QueueDeclarationError = &e
	testTransientEventStreamListenerFailure(t, channel, e.Error())
}

func testTransientEventStreamListenerFailure(t *testing.T, channel *MockAmqpChannel, expectedError string) {
	conn := mockConnection(channel)

	uuid.SetRand(badRand{})
	err := TransientEventStreamListener("key", func(i interface{}, headers Headers) (interface{}, error) {
		return nil, errors.New("failed")
	}, reflect.TypeOf(Message{}))(conn)

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
	published interface{}
}

func (m *mockPublisher) publish(targetService, routingKey string, msg interface{}) error {
	if m.err != nil {
		return m.err
	}
	m.published = msg
	return nil
}

func (m *mockPublisher) checkPublished(t *testing.T, i interface{}) {
	require.EqualValues(t, m.published, i)
}

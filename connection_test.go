package goamqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func Test_AmqpVersion(t *testing.T) {
	assert.Equal(t, "_unknown_", amqpVersion())
}

func Test_Start_MultipleCallsFails(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{
		qosFn: func(prefetchCount, prefetchSize int, global bool) error {
			assert.Equal(t, 20, prefetchCount)
			return nil
		},
	}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
	}
	err := conn.Start()
	assert.NoError(t, err)
	err = conn.Start()
	assert.Error(t, err)
	assert.EqualError(t, err, "already started")
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
	assert.Error(t, err)
	assert.EqualError(t, err, "error setting qos")
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
		EventStreamListener("test", func(i interface{}) bool {
			return false
		}, reflect.TypeOf(Message{})))
	assert.Error(t, err)
	assert.EqualError(t, err, "failed to create consumer for queue events.topic.exchange.queue.test. error consuming queue")
}

func Test_Start_WithDelayedMessaging_Runs_First(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	mockChannel := &MockAmqpChannel{}
	conn := &Connection{
		serviceName: "test",
		connection:  mockAmqpConnection,
		channel:     mockChannel,
	}
	err := conn.Start(
		func(conn *Connection) error {
			assert.True(t, conn.config.DelayedMessage)
			return nil
		},
		WithDelayedMessaging(),
	)
	assert.NoError(t, err)
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
					assert.Equal(t, 20, prefetchCount)
				} else {
					assert.Equal(t, 1, prefetchCount)
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
	assert.NoError(t, err)
}

func Test_Start_ConnectionFail(t *testing.T) {
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return nil, errors.New("failed to connect")
	}
	conn, err := NewFromURL("", "amqp://user:password@localhost:67333/a")
	assert.NoError(t, err)
	err = conn.Start()
	assert.Error(t, err)
	assert.EqualError(t, err, "failed to connect")
}

func Test_CloseCallsUnderlyingCloseMethod(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Close()
	assert.NoError(t, err)
	assert.Equal(t, true, conn.connection.(*MockAmqpConnection).CloseCalled)
}

func Test_CloseListener(t *testing.T) {
	listener := make(chan error)
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := CloseListener(listener)(conn)
	assert.NoError(t, err)
	assert.Equal(t, true, channel.NotifyCloseCalled)
}

func Test_ConnectToAmqpUrl_Ok(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{ChannelConnected: true}
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return mockAmqpConnection, nil
	}
	conn := Connection{config: AmqpConfig{
		Username:       "user",
		Password:       "password",
		Host:           "localhost",
		Port:           12345,
		VHost:          "vhost",
		DelayedMessage: false,
	}}
	err := conn.connectToAmqpURL()
	assert.NoError(t, err)
	assert.Equal(t, mockAmqpConnection, conn.connection)
	assert.NotNil(t, conn.channel)
}

func Test_ConnectToAmqpUrl_ConnectionFailed(t *testing.T) {
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return nil, errors.New("failure to connect")
	}
	conn := Connection{}
	err := conn.connectToAmqpURL()
	assert.Error(t, err)
	assert.Nil(t, conn.connection)
	assert.Nil(t, conn.channel)
}

func Test_ConnectToAmqpUrl_FailToGetChannel(t *testing.T) {
	mockAmqpConnection := &MockAmqpConnection{}
	dialAmqp = func(url string, cfg amqp.Config) (amqpConnection, error) {
		return mockAmqpConnection, nil
	}
	conn := Connection{}
	err := conn.connectToAmqpURL()
	assert.Error(t, err)
	assert.Nil(t, conn.connection)
	assert.Nil(t, conn.channel)
}

func Test_FailingSetupFunc(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(func(c *Connection) error { return nil }, func(c *Connection) error { return fmt.Errorf("error message") })
	assert.EqualError(t, err, "setup function <gitlab.com/sparetimecoders/goamqp.Test_FailingSetupFunc.func2> failed, error message")
}

func Test_NewFromURL_InvalidURL(t *testing.T) {
	c, err := NewFromURL("test", "amqp://")
	assert.Nil(t, c)
	assert.EqualError(t, err, "connection url is invalid, amqp://")
}

func Test_NewFromURL_ValidURL(t *testing.T) {
	c, err := NewFromURL("test", "amqp://user:password@localhost:5672/")
	assert.NotNil(t, c)
	assert.NoError(t, err)
}

func Test_NewFromConfig(t *testing.T) {
	config, err := ParseAmqpURL("amqp://user:password@localhost:5672/")
	assert.NoError(t, err)
	c := New("test", config)
	assert.NotNil(t, c)
}

func Test_AmqpConfig(t *testing.T) {
	assert.Equal(t, fmt.Sprintf("servicename#_unknown_#@%s", hostName()), amqpConfig("servicename").Properties["connection_name"])
}

func Test_QueueDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := queueDeclare(channel, "test")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "test", durable: true, autoDelete: false, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_TransientQueueDeclare(t *testing.T) {
	channel := NewMockAmqpChannel()
	err := transientQueueDeclare(channel, "test")
	assert.NoError(t, err)

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "test", durable: false, autoDelete: true, noWait: false, args: amqp.Table{"x-expires": int(deleteQueueAfter.Seconds() * 1000)}}, channel.QueueDeclarations[0])
}

func Test_ExchangeDeclare_DelayedMessaging(t *testing.T) {
	channel := NewMockAmqpChannel()

	conn := mockConnection(channel)
	conn.config.DelayedMessage = true

	err := conn.exchangeDeclare(channel, "name", "topic")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "name", kind: "x-delayed-message", durable: true, autoDelete: false, noWait: false, args: amqp.Table{"x-delayed-type": "topic"}}, channel.ExchangeDeclarations[0])
}

func Test_ExchangeDeclare_NoDelayedMessaging(t *testing.T) {
	channel := NewMockAmqpChannel()

	conn := mockConnection(channel)
	conn.config.DelayedMessage = false

	err := conn.exchangeDeclare(channel, "name", "topic")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "name", kind: "topic", durable: true, autoDelete: false, noWait: false, args: amqp.Table{}}, channel.ExchangeDeclarations[0])
}

func Test_Consume(t *testing.T) {
	channel := NewMockAmqpChannel()
	_, err := consume(channel, "q")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.Consumers))
	assert.Equal(t, Consumer{queue: "q",
		consumer: "", autoAck: false, exclusive: false, noLocal: false, noWait: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_Publish(t *testing.T) {
	channel := NewMockAmqpChannel()
	headers := amqp.Table{}
	headers["key"] = "value"
	c := Connection{
		channel:       channel,
		messageLogger: NopLogger(),
	}
	err := c.publishMessage(Message{true}, "key", "exchange", headers)
	assert.NoError(t, err)

	publish := <-channel.Published
	assert.Equal(t, "key", publish.key)
	assert.Equal(t, "exchange", publish.exchange)
	assert.Equal(t, false, publish.immediate)
	assert.Equal(t, false, publish.mandatory)

	msg := publish.msg
	assert.Equal(t, "", msg.Type)
	assert.Equal(t, "application/json", msg.ContentType)
	assert.Equal(t, "", msg.AppId)
	assert.Equal(t, "", msg.ContentEncoding)
	assert.Equal(t, "", msg.CorrelationId)
	assert.Equal(t, uint8(2), msg.DeliveryMode)
	assert.Equal(t, "", msg.Expiration)
	assert.Equal(t, "value", msg.Headers["key"])
	assert.Equal(t, "", msg.ReplyTo)

	body := &Message{}
	_ = json.Unmarshal(msg.Body, &body)
	assert.Equal(t, &Message{true}, body)
	assert.Equal(t, "", msg.UserId)
	assert.Equal(t, uint8(0), msg.Priority)
	assert.Equal(t, "", msg.MessageId)
}

func Test_DivertToMessageHandler(t *testing.T) {

	acker := MockAcknowledger{
		Acks:    make(chan Ack, 3),
		Nacks:   make(chan Nack, 1),
		Rejects: make(chan Reject, 1),
	}
	channel := MockAmqpChannel{Published: make(chan Publish, 1)}

	handlers := make(map[string]messageHandlerInvoker)
	msgInvoker := messageHandlerInvoker{
		EventType: reflect.TypeOf(Message{}),
		msgHandler: func(i interface{}) bool {
			return i.(*Message).Ok
		},
	}
	reqResInvoker := messageHandlerInvoker{
		EventType:        reflect.TypeOf(Message{}),
		ResponseExchange: "response",
		ResponseHandler: func(i interface{}) (interface{}, bool) {
			return "", i.(*Message).Ok
		},
	}

	handlers["key1"] = msgInvoker
	handlers["key2"] = msgInvoker
	handlers["key3"] = reqResInvoker

	queueDeliveries := make(chan amqp.Delivery, 5)

	queueDeliveries <- delivery(acker, "key1", true)
	queueDeliveries <- delivery(acker, "key2", true)
	queueDeliveries <- delivery(acker, "key2", false)
	queueDeliveries <- delivery(acker, "key3", true)
	queueDeliveries <- delivery(acker, "missing", true)
	close(queueDeliveries)

	c := Connection{
		started:       true,
		channel:       &channel,
		messageLogger: NopLogger(),
	}
	c.divertToMessageHandlers(queueDeliveries, handlers)

	assert.Equal(t, 3, len(acker.Acks))
	assert.Equal(t, 1, len(acker.Nacks))
	assert.Equal(t, 1, len(acker.Rejects))
	assert.Equal(t, Publish{exchange: "response", key: "", mandatory: false, immediate: false, msg: amqp.Publishing{Headers: amqp.Table{"service": interface{}(nil)}, ContentType: "application/json", ContentEncoding: "", DeliveryMode: 0x2, Priority: 0x0, CorrelationId: "", ReplyTo: "", Expiration: "", MessageId: "", Type: "", UserId: "", AppId: "", Body: []uint8{0x22, 0x22}}}, <-channel.Published)

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
	assert.Equal(t, Ack{tag: 0x0, multiple: false}, <-testHandleMessage("{}", true).Acks)
}

func Test_HandleMessage_Nack_WhenUnhandled(t *testing.T) {
	assert.Equal(t, Nack{tag: 0x0, multiple: false, requeue: true}, <-testHandleMessage("{}", false).Nacks)
}

func Test_HandleMessage_Reject_IfParseFails(t *testing.T) {
	assert.Equal(t, Reject{tag: 0x0, requeue: false}, <-testHandleMessage("", true).Rejects)
}

func testHandleMessage(json string, handle bool) MockAcknowledger {
	type Message struct{}
	acker := NewMockAcknowledger()
	delivery := amqp.Delivery{
		Body:         []byte(json),
		Acknowledger: &acker,
	}
	c := &Connection{
		messageLogger: NopLogger(),
	}
	c.handleMessage(delivery, func(i interface{}) bool {
		return handle
	}, reflect.TypeOf(Message{}),
		"routingkey")
	return acker
}

func Test_HandleRequestResponse_Reject_IfParseFails(t *testing.T) {
	_, acker := testHandleRequestResponse("", true, false)
	assert.Equal(t, Reject{tag: 0x0, requeue: false}, <-acker.Rejects)
}

func Test_HandleRequestResponse_NackRequeuedWhenHandlingFailed(t *testing.T) {
	_, acker := testHandleRequestResponse("{}", false, false)
	assert.Equal(t, Nack{tag: 0x0, multiple: false, requeue: true}, <-acker.Nacks)
}

func Test_HandleRequestResponse_NackNotRequeuedWhenPublishFails(t *testing.T) {
	_, acker := testHandleRequestResponse("{}", true, true)
	assert.Equal(t, Nack{tag: 0x0, multiple: false, requeue: false}, <-acker.Nacks)
}

func Test_HandleRequestResponse_AckWhenSuccess(t *testing.T) {
	channel, acker := testHandleRequestResponse("{}", true, false)
	assert.Equal(t, Ack{tag: 0x0, multiple: false}, <-acker.Acks)
	assert.Equal(t, Publish{exchange: "", key: "ok", mandatory: false, immediate: false, msg: amqp.Publishing{Headers: amqp.Table{"service": interface{}(nil), "x-delay": "1000"}, ContentType: "application/json", ContentEncoding: "", DeliveryMode: 0x2, Body: []uint8{0x7b, 0x22, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x22, 0x7d}}}, <-channel.Published)

}

func testHandleRequestResponse(json string, handled, publishFail bool) (MockAmqpChannel, MockAcknowledger) {
	acker := NewMockAcknowledger()
	channel := MockAmqpChannel{Published: make(chan Publish, 1)}
	delivery := amqp.Delivery{
		Body:         []byte(json),
		Acknowledger: &acker,
	}
	invoker := messageHandlerInvoker{
		EventType: reflect.TypeOf(Message{}),
		ResponseHandler: func(i2 interface{}) (i interface{}, b bool) {
			return Delayed{}, handled
		},
	}
	if publishFail {
		invoker.queueRoutingKey = queueRoutingKey{
			RoutingKey: "failed",
		}
	} else {
		invoker.queueRoutingKey = queueRoutingKey{
			RoutingKey: "ok",
		}
	}
	c := &Connection{
		channel:       &channel,
		messageLogger: NopLogger(),
	}

	c.handleRequestResponse(delivery, invoker, "r")
	return channel, acker
}

func Test_EventStreamPublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"}, Route{DelayedTestMessage{}, "key"})
	err := EventStreamPublisher(p)(conn)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "topic"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 0, len(channel.QueueDeclarations))
	assert.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	assert.NoError(t, err)

	published := <-channel.Published
	assert.Equal(t, "key", published.key)

	err = p.Publish(DelayedTestMessage{"test"})
	assert.NoError(t, err)
	published = <-channel.Published

	assert.Equal(t, 2, len(published.msg.Headers))
	assert.Equal(t, "1000", published.msg.Headers["x-delay"])
	assert.Equal(t, "svc", published.msg.Headers["service"])
}

func Test_EventStreamPublisher_FailedToCreateExchange(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"})
	e := errors.New("failed to create exchange")
	channel.ExchangeDeclarationError = &e
	err := EventStreamPublisher(p)(conn)
	assert.Error(t, err)
	assert.EqualError(t, err, e.Error())
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
	assert.NotNil(t, conn.messageLogger)

	err := p.Publish(TestMessage{"test", true})
	assert.NoError(t, err)
	<-channel.Published

	assert.Equal(t, true, logger.outgoing)
	assert.Equal(t, "routingkey", logger.routingKey)
	assert.Equal(t, reflect.TypeOf(TestMessage{}), logger.eventType)
}

func Test_UseMessageLogger_Nil(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "routingkey"})
	err := conn.Start(
		UseMessageLogger(nil),
		ServicePublisher("service", p),
	)
	assert.Error(t, err)
}

func Test_UseMessageLogger_Default(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "routingkey"})
	err := conn.Start(
		ServicePublisher("service", p),
	)
	assert.NoError(t, err)
	assert.NotNil(t, conn.messageLogger)
}

func Test_EventStreamListener(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := conn.Start(EventStreamListener("key", func(i interface{}) bool {
		return true
	}, reflect.TypeOf(TestMessage{})))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "topic"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	assert.Equal(t, 1, len(channel.BindingDeclarations))
	assert.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc", noWait: false, exchange: "events.topic.exchange", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])

	assert.Equal(t, 1, len(channel.Consumers))
	assert.Equal(t, Consumer{queue: "events.topic.exchange.queue.svc", consumer: "", noWait: false, noLocal: false, exclusive: false, autoAck: false, args: amqp.Table{}}, channel.Consumers[0])
}

func Test_ServicePublisher_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"})

	err := ServicePublisher("svc", p)(conn)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 0, len(channel.QueueDeclarations))
	assert.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	assert.NoError(t, err)
	published := <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "svc.direct.exchange.request", published.exchange)
}

func Test_ServicePublisher_Multiple(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"}, Route{DelayedTestMessage{}, "key2"})

	err := ServicePublisher("svc", p)(conn)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 0, len(channel.QueueDeclarations))
	assert.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(TestMessage{"test", true})
	assert.NoError(t, err)
	err = p.Publish(DelayedTestMessage{"delayed"})
	assert.NoError(t, err)
	err = p.Publish(TestMessage{"test2", false})
	assert.NoError(t, err)
	published := <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "svc.direct.exchange.request", published.exchange)
	published = <-channel.Published
	assert.Equal(t, "key2", published.key)
	assert.Equal(t, "svc.direct.exchange.request", published.exchange)
	published = <-channel.Published
	assert.Equal(t, "key", published.key)
	assert.Equal(t, "{\"Msg\":\"test2\",\"Success\":false}", string(published.msg.Body))

}

func Test_ServicePublisher_NoMatchingRoute(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher(Route{TestMessage{}, "key"})

	err := ServicePublisher("svc", p)(conn)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 0, len(channel.QueueDeclarations))
	assert.Equal(t, 0, len(channel.BindingDeclarations))

	err = p.Publish(&DelayedTestMessage{"test"})
	assert.EqualError(t, err, "no routingkey configured for message of type *goamqp.DelayedTestMessage")
}

func Test_ServicePublisher_ExchangeDeclareFail(t *testing.T) {
	e := errors.New("failed")
	channel := NewMockAmqpChannel()
	channel.ExchangeDeclarationError = &e
	conn := mockConnection(channel)

	p := NewPublisher(Route{TestMessage{}, "key"})
	err := ServicePublisher("svc", p)(conn)
	assert.Error(t, err)
	assert.EqualError(t, err, e.Error())
}

func Test_RequestResponseHandler(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := RequestResponseHandler("key", func(i interface{}) (interface{}, bool) {
		return Message{}, true
	}, reflect.TypeOf(Message{}))(conn)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "svc.headers.exchange.response", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "headers"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[0])
	assert.Equal(t, ExchangeDeclaration{name: "svc.direct.exchange.request", noWait: false, internal: false, autoDelete: false, durable: true, args: amqp.Table{"x-delayed-type": "direct"}, kind: "x-delayed-message"}, channel.ExchangeDeclarations[1])

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "svc.direct.exchange.request.queue", noWait: false, autoDelete: false, durable: true, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	assert.Equal(t, 1, len(channel.BindingDeclarations))
	assert.Equal(t, BindingDeclaration{queue: "svc.direct.exchange.request.queue", noWait: false, exchange: "svc.direct.exchange.request", key: "key", args: amqp.Table{}}, channel.BindingDeclarations[0])
}

func Test_TransientEventStreamListener_Ok(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	uuid.SetRand(badRand{})
	err := TransientEventStreamListener("key", func(i interface{}) bool {
		return false
	}, reflect.TypeOf(Message{}))(conn)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(channel.BindingDeclarations))
	assert.Equal(t, BindingDeclaration{queue: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", key: "key", exchange: "events.topic.exchange", noWait: false, args: amqp.Table{}}, channel.BindingDeclarations[0])

	assert.Equal(t, 1, len(channel.ExchangeDeclarations))
	assert.Equal(t, ExchangeDeclaration{name: "events.topic.exchange", kind: "x-delayed-message", durable: true, autoDelete: false, internal: false, noWait: false, args: amqp.Table{"x-delayed-type": "topic"}}, channel.ExchangeDeclarations[0])

	assert.Equal(t, 1, len(channel.QueueDeclarations))
	assert.Equal(t, QueueDeclaration{name: "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", durable: false, autoDelete: true, noWait: false, args: amqp.Table{"x-expires": 432000000}}, channel.QueueDeclarations[0])

	assert.Equal(t, 1, len(conn.handlers))
	key := queueRoutingKey{
		Queue:      "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f",
		RoutingKey: "key",
	}
	invoker := conn.handlers[key]
	assert.Equal(t, reflect.TypeOf(Message{}), invoker.EventType)
	assert.Equal(t, "key", invoker.RoutingKey)
	assert.Equal(t, "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", invoker.Queue)
	assert.Equal(t, "", invoker.ResponseExchange)
	assert.Equal(t, key, invoker.queueRoutingKey)
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
	err := TransientEventStreamListener("key", func(i interface{}) bool {
		return false
	}, reflect.TypeOf(Message{}))(conn)

	assert.EqualError(t, err, "routingkey key for queue events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f already assigned to handler for type %!s(<nil>), cannot assign goamqp.Message")
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
	err := TransientEventStreamListener("key", func(i interface{}) bool {
		return false
	}, reflect.TypeOf(Message{}))(conn)

	assert.EqualError(t, err, expectedError)
}

func Test_PublishNotify(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	notifier := make(chan amqp.Confirmation)
	err := PublishNotify(notifier)(conn)
	assert.NoError(t, err)
	assert.Equal(t, &notifier, channel.Confirms)
	assert.Equal(t, true, channel.ConfirmCalled)
}

type Delayed struct {
	Name string `json:"name"`
}

func (Delayed) TTL() time.Duration { return time.Second }

type Message struct {
	Ok bool
}

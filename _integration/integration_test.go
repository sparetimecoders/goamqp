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

package _integration

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	. "github.com/sparetimecoders/goamqp"
)

var (
	amqpUser          = "user"
	amqpPasswod       = "password"
	amqpHost          = "localhost"
	amqpPort          = 5672
	amqpAdminPort     = 15672
	amqpURL           = fmt.Sprintf("amqp://%s:%s@%s:%d", amqpUser, amqpPasswod, amqpHost, amqpPort)
	serverServiceName = "server"
)

type IntegrationTestSuite struct {
	suite.Suite
	admin *amqpAdmin
}

func (suite *IntegrationTestSuite) SetupTest() {
	suite.admin = AmqpAdmin(amqpHost, amqpAdminPort, amqpUser, amqpPasswod, uuid.New().String())
	err := suite.admin.CreateVHost()
	require.NoError(suite.T(), err)
}

func (suite *IntegrationTestSuite) TearDownTest() {
	err := suite.admin.DeleteVHost()
	require.NoError(suite.T(), err)
}

func TestIntegration(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) Test_ServiceRequestConsumer() {
	conn := createConnection(suite, serverServiceName,
		ServiceRequestConsumer("key", func(msg any, headers Headers) (response any, err error) {
			return nil, nil
		}, Incoming{}),
	)
	defer conn.Close()

	exchanges, err := suite.admin.GetExchanges(true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Exchange{{
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "server.direct.exchange.request",
		Type:       "direct",
		Vhost:      suite.admin.vhost,
	}, {
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "server.headers.exchange.response",
		Type:       "headers",
		Vhost:      suite.admin.vhost,
	}}, exchanges)

	queues, err := suite.admin.GetQueues()
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Queue{{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "server.direct.exchange.request.queue",
		Vhost:                suite.admin.vhost,
	}}, queues)

	bindings, err := suite.admin.GetBindings(queues[0].Name, true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Binding{{
		Source:          "server.direct.exchange.request",
		Vhost:           suite.admin.vhost,
		Destination:     queues[0].Name,
		DestinationType: "queue",
		RoutingKey:      "key",
		Arguments:       struct{}{},
	}}, bindings)
}

func (suite *IntegrationTestSuite) Test_RequestResponse() {
	closer := make(chan bool)
	var serverReceived *Incoming
	routingKey := "key"
	clientQuery := "test"
	server := createConnection(suite, serverServiceName,
		RequestResponseHandler(
			routingKey,
			func(msg any, headers Headers) (any, error) {
				serverReceived = msg.(*Incoming)
				return IncomingResponse{Value: serverReceived.Query}, nil
			}, Incoming{}))
	defer server.Close()

	publish, err := NewPublisher(Route{
		Type: Incoming{},
		Key:  routingKey,
	})
	require.NoError(suite.T(), err)

	var clientReceived *IncomingResponse
	client := createConnection(suite, "client",
		ServicePublisher(serverServiceName, publish),
		ServiceResponseConsumer(serverServiceName, routingKey, func(msg any, headers Headers) (any, error) {
			clientReceived = msg.(*IncomingResponse)
			closer <- true
			return nil, nil
		}, IncomingResponse{}))
	defer client.Close()

	err = publish.PublishWithContext(context.Background(), &Incoming{Query: clientQuery})
	require.NoError(suite.T(), err)

	<-closer
	require.Equal(suite.T(), &IncomingResponse{Value: clientQuery}, clientReceived)
	require.Equal(suite.T(), &Incoming{Query: clientQuery}, serverReceived)

	// Verify exchanges
	exchanges, err := suite.admin.GetExchanges(true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Exchange{{
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "server.direct.exchange.request",
		Type:       "direct",
		Vhost:      suite.admin.vhost,
	}, {
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "server.headers.exchange.response",
		Type:       "headers",
		Vhost:      suite.admin.vhost,
	}}, exchanges)

	// Verify queues and bindings
	serverQueue, err := suite.admin.GetQueue("server.direct.exchange.request.queue")
	require.NoError(suite.T(), err)
	clientQueue, err := suite.admin.GetQueue("server.headers.exchange.response.queue.client")
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), &Queue{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "server.direct.exchange.request.queue",
		Vhost:                suite.admin.vhost,
	}, serverQueue)

	requestBinding, err := suite.admin.GetBindings("server.direct.exchange.request.queue", true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Binding{
		{
			Source:          "server.direct.exchange.request",
			Vhost:           suite.admin.vhost,
			Destination:     "server.direct.exchange.request.queue",
			DestinationType: "queue",
			RoutingKey:      routingKey,
			Arguments:       struct{}{},
		}}, requestBinding)

	require.Equal(suite.T(), &Queue{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "server.headers.exchange.response.queue.client",
		Vhost:                suite.admin.vhost,
	}, clientQueue)

	responseBinding, err := suite.admin.GetBindings("server.headers.exchange.response.queue.client", true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Binding{
		{
			Source:          "server.headers.exchange.response",
			Vhost:           suite.admin.vhost,
			Destination:     "server.headers.exchange.response.queue.client",
			DestinationType: "queue",
			RoutingKey:      routingKey,
			Arguments:       struct{}{},
		}}, responseBinding)

}

func (suite *IntegrationTestSuite) Test_EventStream_MultipleConsumers() {
	closer := make(chan bool, 2)
	routingKey := "key1"
	clientQuery := "test"
	publish, err := NewPublisher(
		Route{
			Type: Incoming{},
			Key:  routingKey,
		})
	require.NoError(suite.T(), err)
	server := createConnection(suite, serverServiceName,
		EventStreamPublisher(publish))
	defer server.Close()

	var client1Received *Incoming
	var client2Received *Incoming
	client1 := createConnection(suite, "client1", EventStreamConsumer(routingKey, func(msg any, headers Headers) (response any, err error) {
		client1Received = msg.(*Incoming)
		closer <- true
		return nil, nil
	}, Incoming{}))
	defer client1.Close()
	client2 := createConnection(suite, "client2", EventStreamConsumer(routingKey, func(msg any, headers Headers) (response any, err error) {
		client2Received = msg.(*Incoming)
		closer <- true
		return nil, nil
	}, Incoming{}))
	defer client2.Close()

	err = publish.PublishWithContext(context.Background(), &Incoming{Query: clientQuery})
	require.NoError(suite.T(), err)

	go forceClose(closer, 3)
	<-closer
	<-closer
	require.Equal(suite.T(), &Incoming{Query: clientQuery}, client1Received)
	require.Equal(suite.T(), &Incoming{Query: clientQuery}, client2Received)

	// Verify exchanges
	exchanges, err := suite.admin.GetExchanges(true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Exchange{{
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "events.topic.exchange",
		Type:       "topic",
		Vhost:      suite.admin.vhost,
	}}, exchanges)

	// Verify queues and bindings
	client1Queue, err := suite.admin.GetQueue("events.topic.exchange.queue.client1")
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), &Queue{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "events.topic.exchange.queue.client1",
		Vhost:                suite.admin.vhost,
	}, client1Queue)

	client1Binding, err := suite.admin.GetBindings(client1Queue.Name, true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Binding{
		{
			Source:          "events.topic.exchange",
			Vhost:           suite.admin.vhost,
			Destination:     "events.topic.exchange.queue.client1",
			DestinationType: "queue",
			RoutingKey:      routingKey,
			Arguments:       struct{}{},
		}}, client1Binding)

	client2Queue, err := suite.admin.GetQueue("events.topic.exchange.queue.client2")
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), &Queue{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "events.topic.exchange.queue.client2",
		Vhost:                suite.admin.vhost,
	}, client2Queue)

	client2Binding, err := suite.admin.GetBindings(client2Queue.Name, true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Binding{
		{
			Source:          "events.topic.exchange",
			Vhost:           suite.admin.vhost,
			Destination:     "events.topic.exchange.queue.client2",
			DestinationType: "queue",
			RoutingKey:      routingKey,
			Arguments:       struct{}{},
		}}, client2Binding)

}

func (suite *IntegrationTestSuite) Test_EventStream() {
	closer := make(chan bool, 2)
	routingKey1 := "key1"
	routingKey2 := "key2"
	clientQuery := "test"
	publish, err := NewPublisher(
		Route{
			Type: Incoming{},
			Key:  routingKey1,
		},
		Route{
			Type: IncomingResponse{},
			Key:  routingKey2,
		})
	require.NoError(suite.T(), err)
	server := createConnection(suite, serverServiceName,
		EventStreamPublisher(publish))
	defer server.Close()

	var received []any
	client1 := createConnection(suite, "client1",
		TransientEventStreamConsumer(routingKey1, func(msg any, headers Headers) (response any, err error) {
			received = append(received, msg)
			closer <- true
			return nil, nil
		}, Incoming{}),
		EventStreamConsumer(routingKey2, func(msg any, headers Headers) (response any, err error) {
			received = append(received, msg)
			closer <- true
			return nil, nil
		}, IncomingResponse{}))
	defer client1.Close()

	err = publish.PublishWithContext(context.Background(), &Incoming{Query: clientQuery})
	require.NoError(suite.T(), err)
	err = publish.PublishWithContext(context.Background(), &IncomingResponse{Value: clientQuery})
	require.NoError(suite.T(), err)

	go forceClose(closer, 3)
	<-closer
	<-closer
	require.Equal(suite.T(), &Incoming{Query: clientQuery}, received[0])
	require.Equal(suite.T(), &IncomingResponse{Value: clientQuery}, received[1])

	// Verify exchanges
	exchanges, err := suite.admin.GetExchanges(true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Exchange{{
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "events.topic.exchange",
		Type:       "topic",
		Vhost:      suite.admin.vhost,
	}}, exchanges)

	// Verify queues and bindings
	queuesBeforeClose, err := suite.admin.GetQueues()
	for _, q := range queuesBeforeClose {
		bindings, err := suite.admin.GetBindings(q.Name, true)
		require.NoError(suite.T(), err)
		require.Equal(suite.T(), 1, len(bindings))
		binding := bindings[0]
		if q.Name == "events.topic.exchange.queue.client1" {
			require.Equal(suite.T(), Queue{
				Arguments: QueueArguments{
					XExpires: int(5 * 24 * time.Hour.Milliseconds()),
				},
				AutoDelete:           false,
				Durable:              true,
				Exclusive:            false,
				ExclusiveConsumerTag: nil,
				Name:                 q.Name,
				Vhost:                suite.admin.vhost,
			}, q)

			require.Equal(suite.T(), Binding{
				Source:          "events.topic.exchange",
				Vhost:           suite.admin.vhost,
				Destination:     q.Name,
				DestinationType: "queue",
				RoutingKey:      routingKey2,
				Arguments:       struct{}{},
			}, binding)
		} else {
			require.Equal(suite.T(), Queue{
				Arguments: QueueArguments{
					XExpires: int(5 * 24 * time.Hour.Milliseconds()),
				},
				AutoDelete:           true,
				Durable:              false,
				Exclusive:            false,
				ExclusiveConsumerTag: nil,
				Name:                 q.Name,
				Vhost:                suite.admin.vhost,
			}, q)

			require.Equal(suite.T(), Binding{
				Source:          "events.topic.exchange",
				Vhost:           suite.admin.vhost,
				Destination:     q.Name,
				DestinationType: "queue",
				RoutingKey:      routingKey1,
				Arguments:       struct{}{},
			}, binding)

			require.Regexp(suite.T(), regexp.MustCompile("events.topic.exchange.queue.client1-.*"), q.Name)
		}
	}
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), 2, len(queuesBeforeClose))

	client1.Close()

	// Transient queues removed
	queuesAfterClose, err := suite.admin.GetQueues()
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), 1, len(queuesAfterClose))
	require.Equal(suite.T(), "events.topic.exchange.queue.client1", queuesAfterClose[0].Name)
}
func (suite *IntegrationTestSuite) Test_WildcardRoutingKeys() {
	closer := make(chan bool, 2)
	wildcardRoutingKey := "test.#"
	wildcardStarRoutingKey := "1.*.test.*"
	exactMatchRoutingKey := "testing"
	clientQuery := "test"
	publish, err := NewPublisher(
		Route{
			Type: Incoming{},
			Key:  "test.1",
		},
		Route{
			Type: Test{},
			Key:  "1.2.test.2",
		},
		Route{
			Type: IncomingResponse{},
			Key:  exactMatchRoutingKey,
		})
	require.NoError(suite.T(), err)
	server := createConnection(suite, serverServiceName,
		EventStreamPublisher(publish))
	defer server.Close()

	var wildcardStarReceiver []any
	var wildcardReceiver []any
	var exactMatchReceiver []any
	client1 := createConnection(suite, "client1",
		EventStreamConsumer(wildcardRoutingKey, func(msg any, headers Headers) (response any, err error) {
			wildcardReceiver = append(wildcardReceiver, msg)
			closer <- true
			return nil, nil
		}, Incoming{}),
		EventStreamConsumer(wildcardStarRoutingKey, func(msg any, headers Headers) (response any, err error) {
			wildcardStarReceiver = append(wildcardStarReceiver, msg)
			closer <- true
			return nil, nil
		}, Test{}),
		EventStreamConsumer("testing", func(msg any, headers Headers) (response any, err error) {
			exactMatchReceiver = append(exactMatchReceiver, msg)
			closer <- true
			return nil, nil
		}, IncomingResponse{}))
	defer client1.Close()

	err = publish.PublishWithContext(context.Background(), &Test{Test: clientQuery})
	require.NoError(suite.T(), err)
	err = publish.PublishWithContext(context.Background(), &Incoming{Query: clientQuery})
	require.NoError(suite.T(), err)
	err = publish.PublishWithContext(context.Background(), &IncomingResponse{Value: clientQuery})
	require.NoError(suite.T(), err)

	go forceClose(closer, 3)
	<-closer
	<-closer
	<-closer
	require.Equal(suite.T(), &Incoming{Query: clientQuery}, wildcardReceiver[0])
	require.Equal(suite.T(), &Test{Test: clientQuery}, wildcardStarReceiver[0])
	require.Equal(suite.T(), &IncomingResponse{Value: clientQuery}, exactMatchReceiver[0])

	// Verify exchanges
	exchanges, err := suite.admin.GetExchanges(true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []Exchange{{
		AutoDelete: false,
		Durable:    true,
		Internal:   false,
		Name:       "events.topic.exchange",
		Type:       "topic",
		Vhost:      suite.admin.vhost,
	}}, exchanges)

	// Verify queues and bindings
	queuesBeforeClose, err := suite.admin.GetQueues()
	require.Equal(suite.T(), 1, len(queuesBeforeClose))
	q := queuesBeforeClose[0]
	bindings, err := suite.admin.GetBindings(q.Name, true)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), Queue{
		Arguments: QueueArguments{
			XExpires: int(5 * 24 * time.Hour.Milliseconds()),
		},
		AutoDelete:           false,
		Durable:              true,
		Exclusive:            false,
		ExclusiveConsumerTag: nil,
		Name:                 "events.topic.exchange.queue.client1",
		Vhost:                suite.admin.vhost,
	}, q)
	require.ElementsMatch(suite.T(), bindings, []Binding{
		{
			Source:          "events.topic.exchange",
			Vhost:           suite.admin.vhost,
			Destination:     q.Name,
			DestinationType: "queue",
			RoutingKey:      wildcardStarRoutingKey,
			Arguments:       struct{}{},
		},
		{
			Source:          "events.topic.exchange",
			Vhost:           suite.admin.vhost,
			Destination:     q.Name,
			DestinationType: "queue",
			RoutingKey:      wildcardRoutingKey,
			Arguments:       struct{}{},
		},
		{
			Source:          "events.topic.exchange",
			Vhost:           suite.admin.vhost,
			Destination:     q.Name,
			DestinationType: "queue",
			RoutingKey:      exactMatchRoutingKey,
			Arguments:       struct{}{},
		},
	})

	require.NoError(suite.T(), err)

	client1.Close()

	// Transient queues removed
	queuesAfterClose, err := suite.admin.GetQueues()
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), 1, len(queuesAfterClose))
	require.Equal(suite.T(), "events.topic.exchange.queue.client1", queuesAfterClose[0].Name)
}

func createConnection(suite *IntegrationTestSuite, serviceName string, opts ...Setup) *Connection {
	conn, err := NewFromURL(serviceName, fmt.Sprintf("%s/%s", amqpURL, suite.admin.vhost))
	require.NoError(suite.T(), err)
	err = conn.Start(context.Background(), opts...)
	require.NoError(suite.T(), err)
	return conn
}

func forceClose(closer chan bool, seconds int64) {
	time.Sleep(time.Duration(seconds) * time.Second)
	closer <- true
}

type Incoming struct {
	Query string
}

type Test struct {
	Test string
}

type IncomingResponse struct {
	Value string
}

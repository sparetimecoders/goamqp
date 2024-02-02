// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package event_stream

import (
	"context"
	"fmt"
	"os"
	"time"

	. "github.com/sparetimecoders/goamqp"
)

var amqpURL = "amqp://user:password@localhost:5672/test"

func ExampleEventStream() {
	ctx := context.Background()
	if urlFromEnv := os.Getenv("AMQP_URL"); urlFromEnv != "" {
		amqpURL = urlFromEnv
	}
	orderServiceConnection := Must(NewFromURL("order-service", amqpURL))
	orderPublisher := NewPublisher()
	err := orderServiceConnection.Start(ctx,
		EventStreamPublisher(orderPublisher),
	)
	checkError(err)

	shippingService := ShippingService{}
	err = shippingService.Start(ctx)
	checkError(err)

	statService := StatService{}
	err = statService.Start(ctx)
	checkError(err)

	err = orderPublisher.PublishWithContext(context.Background(), OrderCreated{Id: "id"})
	checkError(err)
	err = orderPublisher.PublishWithContext(context.Background(), OrderUpdated{Id: "id"})
	checkError(err)

	time.Sleep(2 * time.Second)
	_ = orderServiceConnection.Close()
	_ = shippingService.Stop()
	_ = statService.Stop()
}

// -- StatService
type StatService struct {
	connection *Connection
}

func (s *StatService) Stop() error {
	return s.connection.Close()
}

func (s *StatService) Start(ctx context.Context) error {
	s.connection = Must(NewFromURL("stat-service", amqpURL))
	return s.connection.Start(ctx,
		EventStreamConsumer("Order.Created", s.handleOrderEvent, OrderCreated{}),
	)
}

func (s *StatService) handleOrderEvent(msg any, headers Headers) (response any, err error) {
	switch msg.(type) {
	case *OrderCreated:
		// Just to make sure the Output is correct in the example...
		time.Sleep(time.Second)
		fmt.Println("Increasing order count")
	default:
		fmt.Println("Unknown message type")
	}
	return nil, nil
}

// -- ShippingService
type ShippingService struct {
	connection *Connection
}

func (s *ShippingService) Stop() error {
	return s.connection.Close()
}

func (s *ShippingService) Start(ctx context.Context) error {
	s.connection = Must(NewFromURL("shipping-service", amqpURL))
	return s.connection.Start(ctx,
		EventStreamConsumer("Order.Created", s.handleOrderEvent, OrderCreated{}),
		EventStreamConsumer("Order.Updated", s.handleOrderEvent, OrderUpdated{}),
	)
}

func (s *ShippingService) handleOrderEvent(msg any, headers Headers) (response any, err error) {
	switch msg.(type) {
	case *OrderCreated:
		fmt.Println("Order created")
	case *OrderUpdated:
		fmt.Println("Order deleted")
	default:
		fmt.Println("Unknown message type")
	}
	return nil, nil
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

type OrderCreated struct {
	Id string
}
type OrderUpdated struct {
	Id string
}

type ShippingUpdated struct {
	Id string
}

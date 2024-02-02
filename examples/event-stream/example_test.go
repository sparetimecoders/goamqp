// Copyright (c) 2024 sparetimecoders
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
	"testing"
	"time"

	. "github.com/sparetimecoders/goamqp"
)

// var amqpURL = "amqp://user:password@localhost:5672/test"
var amqpURL = "amqp://user:password@goodfeed-control-plane.orb.local:5672/test"

func Test_A(t *testing.T) {
	ctx := context.Background()
	if urlFromEnv := os.Getenv("AMQP_URL"); urlFromEnv != "" {
		amqpURL = urlFromEnv
	}
	orderServiceConnection := Must(NewFromURL("order-service", amqpURL))
	orderPublisher := NewPublisher()
	err := orderServiceConnection.Start(ctx,
		EventStreamPublisher(orderPublisher),
		WithTypeMapping("Order.Created", OrderCreated{}),
		WithTypeMapping("Order.Updated", OrderUpdated{}),
	)
	checkError(err)

	shippingService := ShippingService{}
	err = shippingService.Start(ctx)
	checkError(err)

	statService := StatService{}
	err = statService.Start(ctx)
	checkError(err)

	err = orderPublisher.Publish(context.Background(), OrderCreated{Id: "id"})
	checkError(err)
	err = orderPublisher.Publish(context.Background(), OrderUpdated{Id: "id", Data: "data"})
	checkError(err)
	time.Sleep(2 * time.Second)
	_ = orderServiceConnection.Close()
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
		WithHandler("Order.Created", s.handleOrderCreated),
		WithHandler("Order.Updated", s.handleOrderUpdated),
	)
}

func (s *StatService) handleOrderUpdated(ctx context.Context, msg ConsumableEvent[OrderUpdated]) (response any, err error) {
	fmt.Printf("Updated order id, %s - %s\n", msg.Payload.Id, msg.Payload.Data)
	return nil, nil
}
func (s *StatService) handleOrderCreated(ctx context.Context, msg ConsumableEvent[OrderCreated]) (response any, err error) {
	// Just to make sure the Output is correct in the example...
	fmt.Printf("Created order, %s\n", msg.Payload.Id)
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
		WithTypeMapping("Order.Created", OrderCreated{}),
		WithTypeMapping("Order.Updated", OrderUpdated{}),
		WithHandler("#", s.connection.TypeMappingHandler(func(ctx context.Context, event any) (any, error) {
			return s.handleOrderEvent(ctx, event)
		}),
		))
}

func (s *ShippingService) handleOrderEvent(ctx context.Context, msg any) (response any, err error) {
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
	Id   string
	Data string
}

type ShippingUpdated struct {
	Id string
}

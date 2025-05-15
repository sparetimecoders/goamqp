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

package event_stream

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/sparetimecoders/goamqp"
)

var amqpURL = "amqp://user:password@localhost:5672"

func Example_event_stream() {
	ctx := context.Background()
	if urlFromEnv := os.Getenv("AMQP_URL"); urlFromEnv != "" {
		amqpURL = urlFromEnv
	}
	orderServiceConnection := goamqp.Must(goamqp.NewFromURL("order-service", amqpURL))
	orderPublisher := goamqp.NewPublisher()

	err := orderServiceConnection.Start(ctx,
		goamqp.EventStreamPublisher(orderPublisher),
	)
	checkError(err)

	shippingService := ShippingService{}
	err = shippingService.Start(ctx)
	checkError(err)

	statService := StatService{}
	err = statService.Start(ctx)
	checkError(err)

	err = orderPublisher.Publish(context.Background(), "Order.Created", OrderCreated{Id: "id"})
	checkError(err)
	err = orderPublisher.Publish(context.Background(), "Order.Updated", OrderUpdated{Id: "id", Data: "data"})
	checkError(err)
	time.Sleep(2 * time.Second)
	_ = orderServiceConnection.Close()
	_ = statService.Stop()

	fmt.Println(statService.output)
	fmt.Println(shippingService.output)
	// Output:
	// [Created order: id Updated order id: id - data]
	// [Order created Order deleted]
}

// -- StatService
type StatService struct {
	connection *goamqp.Connection
	output     []string
}

func (s *StatService) Stop() error {
	return s.connection.Close()
}

func (s *StatService) Start(ctx context.Context) error {
	s.connection = goamqp.Must(goamqp.NewFromURL("stat-service", amqpURL))
	return s.connection.Start(ctx,
		goamqp.EventStreamConsumer("Order.Created", s.handleOrderCreated),
		goamqp.EventStreamConsumer("Order.Updated", s.handleOrderUpdated),
	)
}

func (s *StatService) handleOrderUpdated(ctx context.Context, msg goamqp.ConsumableEvent[OrderUpdated]) error {
	s.output = append(s.output, fmt.Sprintf("Updated order id: %s - %s", msg.Payload.Id, msg.Payload.Data))
	return nil
}

func (s *StatService) handleOrderCreated(ctx context.Context, msg goamqp.ConsumableEvent[OrderCreated]) error {
	s.output = append(s.output, fmt.Sprintf("Created order: %s", msg.Payload.Id))
	return nil
}

// -- ShippingService
type ShippingService struct {
	connection *goamqp.Connection
	output     []string
}

func (s *ShippingService) Stop() error {
	return s.connection.Close()
}

func (s *ShippingService) Start(ctx context.Context) error {
	s.connection = goamqp.Must(goamqp.NewFromURL("shipping-service", amqpURL))
	return s.connection.Start(ctx,
		goamqp.EventStreamConsumer("#", goamqp.TypeMappingHandler(func(ctx context.Context, event goamqp.ConsumableEvent[any]) error {
			switch event.Payload.(type) {
			case *OrderCreated:
				s.output = append(s.output, "Order created")
			case *OrderUpdated:
				s.output = append(s.output, "Order deleted")
			}
			return nil
		}, func(ctx context.Context, routingKey string) (reflect.Type, bool) {
			if routingKey == "Order.Created" {
				return reflect.TypeOf(&OrderCreated{}), true
			}
			if routingKey == "Order.Updated" {
				return reflect.TypeOf(&OrderUpdated{}), true
			}

			return nil, false
		})))
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

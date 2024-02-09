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

package request_response

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sparetimecoders/goamqp"
)

var amqpURL = "amqp://user:password@localhost:5672"

func Example_request_response() {
	ctx := context.Background()
	if urlFromEnv := os.Getenv("AMQP_URL"); urlFromEnv != "" {
		amqpURL = urlFromEnv
	}
	routingKey := "key"
	serviceConnection := goamqp.Must(goamqp.NewFromURL("service", amqpURL))
	err := serviceConnection.Start(ctx,
		goamqp.RequestResponseHandler(routingKey, handleRequest),
	)
	checkError(err)

	clientConnection := goamqp.Must(goamqp.NewFromURL("client", amqpURL))
	publisher := goamqp.NewPublisher()

	err = clientConnection.Start(ctx,
		goamqp.WithTypeMapping(routingKey, Request{}),
		goamqp.ServicePublisher("service", publisher),
		goamqp.ServiceResponseConsumer("service", routingKey, handleResponse),
	)
	checkError(err)

	err = publisher.Publish(context.Background(), Request{Data: "test"})
	checkError(err)

	time.Sleep(time.Second)
	_ = serviceConnection.Close()
	_ = clientConnection.Close()

	// Output:
	// Called process with test, returning response {test}
	// Got response, test
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func handleRequest(ctx context.Context, m goamqp.ConsumableEvent[Request]) (any, error) {
	response := Response{Data: m.Payload.Data}
	fmt.Printf("Called process with %v, returning response %v\n", m.Payload.Data, response)
	return response, nil
}

func handleResponse(ctx context.Context, m goamqp.ConsumableEvent[Response]) error {
	fmt.Printf("Got response, %v\n", m.Payload.Data)
	return nil
}

type Request struct {
	Data string
}
type Response struct {
	Data string
}

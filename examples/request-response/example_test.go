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

package request_response

import (
	"context"
	"fmt"
	"os"
	"time"

	. "github.com/sparetimecoders/goamqp"
)

var amqpURL = "amqp://user:password@localhost:5672/test"

func Example_request_response() {
	ctx := context.Background()
	if urlFromEnv := os.Getenv("AMQP_URL"); urlFromEnv != "" {
		amqpURL = urlFromEnv
	}
	routingKey := "key"
	serviceConnection := Must(NewFromURL("service", amqpURL))
	err := serviceConnection.Start(ctx,
		RequestResponseHandler(routingKey, handleRequest, Request{}),
	)
	checkError(err)

	clientConnection := Must(NewFromURL("client", amqpURL))
	publisher := NewPublisher()

	err = clientConnection.Start(ctx,
		ServicePublisher("service", publisher),
		ServiceResponseConsumer("service", routingKey, handleResponse, Response{}),
	)
	checkError(err)

	err = publisher.PublishWithContext(context.Background(), Request{Data: "test"})
	checkError(err)

	time.Sleep(time.Second)
	_ = serviceConnection.Close()
	_ = clientConnection.Close()
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func handleRequest(m any, headers Headers) (any, error) {
	request := m.(*Request)
	response := Response{Data: request.Data}
	fmt.Printf("Called process with %v, returning response %v\n", request.Data, response)
	return response, nil
}

func handleResponse(m any, headers Headers) (any, error) {
	response := m.(*Response)
	fmt.Printf("Got response, returning response %v\n", response.Data)
	return nil, nil
}

type Request struct {
	Data string
}
type Response struct {
	Data string
}

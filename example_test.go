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

package goamqp_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/sparetimecoders/goamqp"
)

func Example() {
	ctx := context.Background()

	amqpURL := "amqp://user:password@localhost:5672/"
	publisher := Must(NewPublisher(Route{Type: IncomingMessage{}, Key: "key"}))

	connection := Must(NewFromURL("service", amqpURL))
	err := connection.Start(ctx,
		EventStreamConsumer("key", process, IncomingMessage{}),
		EventStreamPublisher(publisher),
	)
	checkError(err)
	err = publisher.Publish(IncomingMessage{"OK"})
	checkError(err)
	time.Sleep(time.Second)
	err = connection.Close()
	checkError(err)
	// Output: Called process with OK
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func process(m any, headers Headers) (any, error) {
	fmt.Printf("Called process with %v\n", m.(*IncomingMessage).Data)
	return nil, nil
}

type IncomingMessage struct {
	Data string
}

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
	"fmt"
	"gitlab.com/sparetimecoders/goamqp"
	"log"
	"time"
)

func Example() {

	config := goamqp.AmqpConfig{
		Host:     "localhost",
		Port:     5672,
		Username: "admin",
		Password: "password",
		VHost:    "",
	}
	publisher := make(chan interface{})

	connection, err := goamqp.New("service", config).
		AddEventStreamListener("testkey", &TestIncomingMessageHandler{}).
		AddEventStreamPublisher("testkey", publisher).
		Start()
	if err != nil {
		log.Fatalln("failed to create publisher", err)
	}

	publisher <- IncomingMessage{"FAILED"}
	publisher <- IncomingMessage{"OK"}
	connection.Close()
}

type TestIncomingMessageHandler struct {
	ctx string
}

func (i TestIncomingMessageHandler) Process(m IncomingMessage) bool {
	fmt.Printf("Called process with %v and ctx %v\n", m, i.ctx)
	return true
}

type IncomingMessage struct {
	Url string
}

func (IncomingMessage) TTL() time.Duration {
	return time.Minute
}

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

package go_amqp_test

import (
	"fmt"
	"gitlab.com/sparetimecoders/go_amqp"
	"log"
	"math/rand"
	"time"
)

func Example() {

	config := go_amqp.Config{
		AmqpConfig: &go_amqp.AmqpConfig{
			Host:     "localhost",
			Port:     5672,
			Username: "admin",
			Password: "password",
			VHost:    "",
		},
		DelayedMessageSupported: true,
	}
	connection, err := go_amqp.New(config)
	if err != nil {
		log.Fatalln("failed to connect", err)
	}
	err = connection.NewEventStreamListener("test-service", "testkey", &TestIncomingMessageHandler{})
	if err != nil {
		log.Fatalln("failed to create listener", err)
	}
	p, err := connection.NewEventStreamPublisher("testkey")
	if err != nil {
		log.Fatalln("failed to create publisher", err)
	}

	r := rand.New(rand.NewSource(99))
	for {
		fmt.Println("Sleep")
		time.Sleep(2 * time.Second)
		fmt.Println("Sending")

		if r.Int()%2 == 0 {
			p <- IncomingMessage{"FAILED"}
		} else {
			p <- IncomingMessage{"OK"}
		}
	}
}

type TestIncomingMessageHandler struct {
	ctx string
}

func (TestIncomingMessageHandler) Type() interface{} {
	return IncomingMessage{}
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

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

package goamqp

import (
	"fmt"
	"reflect"
	"runtime"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueBindingConfigSetup is a setup function that takes a QueueBindingConfig and provide custom changes to the
// configuration
type QueueBindingConfigSetup func(config *QueueBindingConfig) error

// QueueBindingConfig is a wrapper around the actual amqp queue configuration
type QueueBindingConfig struct {
	routingKey          string
	handler             wrappedHandler
	queueName           string
	exchangeName        string
	kind                kind
	queueHeaders        amqp.Table
	queueBindingHeaders amqp.Table
}

// AddQueueNameSuffix appends the provided suffix to the queue name
// Useful when multiple queueConsumers are needed for a routing key in the same service
func AddQueueNameSuffix(suffix string) QueueBindingConfigSetup {
	return func(config *QueueBindingConfig) error {
		if suffix == "" {
			return ErrEmptySuffix
		}
		config.queueName = fmt.Sprintf("%s-%s", config.queueName, suffix)
		return nil
	}
}

// DisableSingleActiveConsumer will define the queue as non exclusive and set the x-single-active-consumer header to false
// https://www.rabbitmq.com/docs/consumers#exclusivity
func DisableSingleActiveConsumer() QueueBindingConfigSetup {
	return func(config *QueueBindingConfig) error {
		config.queueHeaders[headerSingleActiveConsumer] = false
		return nil
	}
}

// getQueueBindingConfigSetupFuncName returns the name of the QueueBindingConfigSetup function
func getQueueBindingConfigSetupFuncName(f QueueBindingConfigSetup) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

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

package goamqp

import (
	"fmt"

	"github.com/google/uuid"
)

func eventsExchangeName() string {
	return exchangeName("events", "topic")
}

func exchangeName(svcName, kind string) string {
	return fmt.Sprintf("%s.%s.exchange", svcName, kind)
}

func serviceEventQueueName(service string) string {
	return fmt.Sprintf("%s.queue.%s", eventsExchangeName(), service)
}

func serviceEventRandomQueueName(service string) string {
	return fmt.Sprintf("%s.queue.%s-%s", eventsExchangeName(), service, randomString())
}

func serviceRequestExchangeName(svcName string) string {
	return fmt.Sprintf("%s.direct.exchange.request", svcName)
}

func serviceResponseExchangeName(svcName string) string {
	return fmt.Sprintf("%s.headers.exchange.response", svcName)
}

func serviceRequestQueueName(service string) string {
	return fmt.Sprintf("%s.queue", serviceRequestExchangeName(service))
}

func serviceResponseQueueName(targetService, serviceName string) string {
	return fmt.Sprintf("%s.queue.%s", serviceResponseExchangeName(targetService), serviceName)
}

func randomString() string {
	return uuid.New().String()
}

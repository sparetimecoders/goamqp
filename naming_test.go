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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEventExchangeName(t *testing.T) {
	assert.Equal(t, "events.topic.exchange", topicExchangeName(defaultEventExchangeName))
}

func TestServiceEventQueueName(t *testing.T) {
	assert.Equal(t, "events.topic.exchange.queue.svc", serviceEventQueueName(topicExchangeName(defaultEventExchangeName), "svc"))
}

func TestRequestExchangeName(t *testing.T) {
	assert.Equal(t, "svc.direct.exchange.request", serviceRequestExchangeName("svc"))
}

func TestResponseExchangeName(t *testing.T) {
	assert.Equal(t, "svc.headers.exchange.response", serviceResponseExchangeName("svc"))
}

func TestServiceRequestQueueName(t *testing.T) {
	assert.Equal(t, "svc.direct.exchange.request.queue", serviceRequestQueueName("svc"))
}

func TestServiceResponseQueueName(t *testing.T) {
	assert.Equal(t, "target.headers.exchange.response.queue.svc", serviceResponseQueueName("target", "svc"))
}

func TestEventRandomQueueName(t *testing.T) {
	uuid.SetRand(&badRand{})
	assert.Equal(t, "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", serviceEventRandomQueueName(topicExchangeName(defaultEventExchangeName), "svc"))
}

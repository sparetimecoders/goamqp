package goamqp

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventExchangeName(t *testing.T) {
	assert.Equal(t, "events.topic.exchange", eventsExchangeName())
}

func TestExchangeName(t *testing.T) {
	assert.Equal(t, "svc.direct.exchange", exchangeName("svc", "direct"))
	assert.Equal(t, "svc.topic.exchange", exchangeName("svc", "topic"))
	assert.Equal(t, "svc.headers.exchange", exchangeName("svc", "headers"))
}

func TestServiceEventQueueName(t *testing.T) {
	assert.Equal(t, "events.topic.exchange.queue.svc", serviceEventQueueName("svc"))
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
	assert.Equal(t, "events.topic.exchange.queue.svc-00010203-0405-4607-8809-0a0b0c0d0e0f", serviceEventRandomQueueName( "svc"))
}
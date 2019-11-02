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

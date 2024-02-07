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
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	queue      = "queue"
	exchange   = "exchange"
	result     = "result"
	routingKey = "routing_key"
)

var (
	eventReceivedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_received",
			Help: "Count of AMQP events received",
		}, []string{queue, routingKey},
	)

	eventWithoutHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_without_handler",
			Help: "Count of AMQP events without a handler",
		}, []string{queue, routingKey},
	)

	eventNotParsableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_not_parsable",
			Help: "Count of AMQP events that could not be parsed",
		}, []string{queue, routingKey},
	)

	eventNackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_nack",
			Help: "Count of AMQP events that were not acknowledged",
		}, []string{queue, routingKey},
	)

	eventAckCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_ack",
			Help: "Count of AMQP events that were acknowledged",
		}, []string{queue, routingKey},
	)

	eventProcessedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "amqp_events_processed_duration",
			Help:    "Milliseconds taken to process an event",
			Buckets: []float64{100, 200, 500, 1000, 3000, 5000, 10000},
		}, []string{queue, routingKey, result},
	)

	eventPublishSucceedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_publish_succeed",
			Help: "Count of AMQP events that could be published successfully",
		}, []string{exchange, routingKey},
	)

	eventPublishFailedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amqp_events_publish_failed",
			Help: "Count of AMQP events that could not be published",
		}, []string{exchange, routingKey},
	)
)

func eventReceived(queue string, routingKey string) {
	eventReceivedCounter.WithLabelValues(queue, routingKey).Inc()
}

func eventWithoutHandler(queue string, routingKey string) {
	eventWithoutHandlerCounter.WithLabelValues(queue, routingKey).Inc()
}

func eventNotParsable(queue string, routingKey string) {
	eventNotParsableCounter.WithLabelValues(queue, routingKey).Inc()
}

func eventNack(queue string, routingKey string, milliseconds int64) {
	eventNackCounter.WithLabelValues(queue, routingKey).Inc()

	eventProcessedDuration.
		WithLabelValues(queue, routingKey, "NACK").
		Observe(float64(milliseconds))
}

func eventAck(queue string, routingKey string, milliseconds int64) {
	eventAckCounter.WithLabelValues(queue, routingKey).Inc()

	eventProcessedDuration.
		WithLabelValues(queue, routingKey, "ACK").
		Observe(float64(milliseconds))
}

func eventPublishSucceed(exchange string, routingKey string) {
	eventPublishSucceedCounter.WithLabelValues(exchange, routingKey).Inc()
}

func eventPublishFailed(exchange string, routingKey string) {
	eventPublishFailedCounter.WithLabelValues(exchange, routingKey).Inc()
}

func InitMetrics(registerer prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		eventReceivedCounter,
		eventAckCounter,
		eventNackCounter,
		eventWithoutHandlerCounter,
		eventNotParsableCounter,
		eventProcessedDuration,
		eventPublishSucceedCounter,
		eventPublishFailedCounter,
	}
	for _, collector := range collectors {
		mv, ok := collector.(metricResetter)
		if ok {
			mv.Reset()
		}
		err := registerer.Register(collector)
		if err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			return err
		}
	}
	return nil
}

// CounterVec, MetricVec and HistogramVec have a Reset func
// in order not to cast to each specific type, metricResetter can
// be used to just get access to the Reset func
type metricResetter interface {
	Reset()
}

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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func Test_Metrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, InitMetrics(registry))
	channel := NewMockAmqpChannel()

	err := publishMessage(context.Background(), channel, Message{true}, "key", "exchange", nil)
	require.NoError(t, err)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	var publishedSuccessfully float64
	for _, metric := range metricFamilies {
		if *metric.Name == "amqp_events_publish_succeed" {
			publishedSuccessfully = *metric.GetMetric()[0].GetCounter().Value
		}
	}
	require.Equal(t, 1.0, publishedSuccessfully)
}

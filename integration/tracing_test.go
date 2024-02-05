//go:build integration
// +build integration

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
// MIT License

package _integration

import (
	"context"

	. "github.com/sparetimecoders/goamqp"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func (suite *IntegrationTestSuite) Test_Tracing() {
	closer := make(chan bool)
	var actualTraceID trace.TraceID
	publish := NewPublisher()
	server := createConnection(suite, serverServiceName,
		EventStreamPublisher(publish),
		EventStreamConsumer("key", func(ctx context.Context, event ConsumableEvent[Test]) error {
			actualTraceID = trace.SpanFromContext(ctx).SpanContext().TraceID()
			closer <- true
			return nil
		}),
		WithTypeMapping("key", Test{}),
	)
	defer server.Close()

	// Setup tracing
	otel.SetTracerProvider(tracesdk.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	publishingContext, _ := otel.Tracer("amqp").Start(context.Background(), "publish-test")

	err := publish.Publish(publishingContext, Test{Test: "value"})
	require.NoError(suite.T(), err)
	<-closer

	require.NoError(suite.T(), server.Close())
	require.Equal(suite.T(), trace.SpanFromContext(publishingContext).SpanContext().TraceID(), actualTraceID)
}

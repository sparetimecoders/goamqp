// MIT License
//
// Copyright (c) 2025 sparetimecoders
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
	"errors"
	"fmt"
)

// RequestResponseHandler is a convenience func to set up ServiceRequestConsumer and combines it with
// PublishServiceResponse
func RequestResponseHandler[T any, R any](routingKey string, handler RequestResponseEventHandler[T, R]) Setup {
	return func(c *Connection) error {
		responseHandlerWrapper := responseWrapper(handler, routingKey, func(ctx context.Context, targetService, routingKey string, msg R) error {
			return c.PublishServiceResponse(ctx, targetService, routingKey, msg)
		})
		return ServiceRequestConsumer[T](routingKey, responseHandlerWrapper)(c)
	}
}

func responseWrapper[T, R any](handler RequestResponseEventHandler[T, R], routingKey string, publisher ServiceResponsePublisher[R]) EventHandler[T] {
	return func(ctx context.Context, event ConsumableEvent[T]) (err error) {
		resp, err := handler(ctx, event)
		if err != nil {
			return fmt.Errorf("failed to process message, %w", err)
		}
		service, err := sendingService(event.DeliveryInfo)
		if err != nil {
			return fmt.Errorf("failed to extract service name, %w", err)
		}
		err = publisher(ctx, service, routingKey, resp)
		if err != nil {
			return fmt.Errorf("failed to publish response, %w", err)
		}
		return nil
	}
}

// sendingService returns the name of the service that produced the message
// Can be used to send a handlerResponse, see PublishServiceResponse
func sendingService(di DeliveryInfo) (string, error) {
	if h, exist := di.Headers[headerService]; exist {
		switch v := h.(type) {
		case string:
			return v, nil
		}
	}
	return "", errors.New("no service found")
}

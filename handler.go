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
	"encoding/json"
	"errors"
	"fmt"
)

var ErrParseJSON = errors.New("failed to parse")

// wrappedHandler is internally used to wrap the generic EventHandler
// this is to facilitate adding all the different type of T on the same map
type wrappedHandler func(ctx context.Context, event unmarshalEvent) error

func newWrappedHandler[T any](handler EventHandler[T]) wrappedHandler {
	return func(ctx context.Context, event unmarshalEvent) error {
		consumableEvent := ConsumableEvent[T]{
			Metadata:     event.Metadata,
			DeliveryInfo: event.DeliveryInfo,
		}
		err := json.Unmarshal(event.Payload, &consumableEvent.Payload)
		if err != nil {
			return fmt.Errorf("%v: %w", err, ErrParseJSON)
		}
		return handler(ctx, consumableEvent)
	}
}

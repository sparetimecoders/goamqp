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
	"regexp"
	"strings"
)

// type Handler func(ctx context.Context, event any) error

// EventHandler is the type definition for a function that is used to handle events of a specific type.
// TODO HandlerFunc is used to process an incoming message
// If processing fails, an error should be returned and the message will be re-queued
// The optional response is used automatically when setting up a RequestResponseHandler, otherwise ignored

type (
	EventHandler[T any]                       func(ctx context.Context, event ConsumableEvent[T]) error
	RequestResponseEventHandler[T any, R any] func(ctx context.Context, event ConsumableEvent[T]) (R, error)
)

// Handlers holds the handlers for a certain queue
type Handlers map[string]wrappedHandler

// get returns the handler for the given queue and routing key that matches
func (h *Handlers) get(routingKey string) (wrappedHandler, bool) {
	for mappedRoutingKey, handler := range *h {
		if match(mappedRoutingKey, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

// exists returns the already mapped routing key if it exists (matched by the matches function to support wildcards)
func (h *Handlers) exists(routingKey string) (string, bool) {
	for mappedRoutingKey := range *h {
		if overlaps(routingKey, mappedRoutingKey) {
			return mappedRoutingKey, true
		}
	}
	return "", false
}

func (h *Handlers) add(routingKey string, handler wrappedHandler) {
	(*h)[routingKey] = handler
}

// QueueHandlers holds all handlers for all queues
type QueueHandlers map[string]*Handlers

// Add a handler for the given queue and routing key
func (h *QueueHandlers) Add(queueName, routingKey string, handler wrappedHandler) error {
	queueHandlers, ok := (*h)[queueName]
	if !ok {
		queueHandlers = &Handlers{}
		(*h)[queueName] = queueHandlers
	}

	if mappedRoutingKey, exists := queueHandlers.exists(routingKey); exists {
		return fmt.Errorf("routingkey %s overlaps %s for queue %s, consider using AddQueueNameSuffix", routingKey, mappedRoutingKey, queueName)
	}
	queueHandlers.add(routingKey, handler)
	return nil
}

type QueueWithHandlers struct {
	Name     string
	Handlers *Handlers
}

// Queues returns all queue names for which we have added a handler
func (h *QueueHandlers) Queues() []QueueWithHandlers {
	if h == nil {
		return []QueueWithHandlers{}
	}
	var res []QueueWithHandlers
	for q, h := range *h {
		res = append(res, QueueWithHandlers{Name: q, Handlers: h})
	}
	return res
}

// Handlers returns all the handlers for a given queue, keyed by the routing key
func (h *QueueHandlers) Handlers(queueName string) *Handlers {
	if h == nil {
		return &Handlers{}
	}

	if handlers, ok := (*h)[queueName]; ok {
		return handlers
	}
	return &Handlers{}
}

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

var ErrParseJSON = errors.New("failed to parse")

// overlaps checks if two AMQP binding patterns overlap
func overlaps(p1, p2 string) bool {
	if p1 == p2 {
		return true
	} else if match(p1, p2) {
		return true
	} else if match(p2, p1) {
		return true
	}
	return false
}

// match returns true if the AMQP binding pattern is matching the routing key
func match(pattern string, routingKey string) bool {
	b, err := regexp.MatchString(fixRegex(pattern), routingKey)
	if err != nil {
		return false
	}
	return b
}

// fixRegex converts the AMQP binding key syntax to regular expression
// For example:
// user.* => user\.[^.]*
// user.# => user\..*
func fixRegex(s string) string {
	replace := strings.Replace(strings.Replace(strings.Replace(s, ".", "\\.", -1), "*", "[^.]*", -1), "#", ".*", -1)
	return fmt.Sprintf("^%s$", replace)
}

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

// handlers holds the handlers for a certain queue
type handlers map[string]wrappedHandler

// get returns the handler for the given queue and routing key that matches
func (h *handlers) get(routingKey string) (wrappedHandler, bool) {
	for mappedRoutingKey, handler := range *h {
		if match(mappedRoutingKey, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

// exists returns the already mapped routing key if it exists (matched by the matches function to support wildcards)
func (h *handlers) exists(routingKey string) (string, bool) {
	for mappedRoutingKey := range *h {
		if overlaps(routingKey, mappedRoutingKey) {
			return mappedRoutingKey, true
		}
	}
	return "", false
}

func (h *handlers) add(routingKey string, handler wrappedHandler) {
	(*h)[routingKey] = handler
}

// queueHandlers holds all handlers for all queues
type queueHandlers map[string]*handlers

// add a handler for the given queue and routing key
func (h *queueHandlers) add(queueName, routingKey string, handler wrappedHandler) error {
	queueHandlers, ok := (*h)[queueName]
	if !ok {
		queueHandlers = &handlers{}
		(*h)[queueName] = queueHandlers
	}

	if mappedRoutingKey, exists := queueHandlers.exists(routingKey); exists {
		return fmt.Errorf("routingkey %s overlaps %s for queue %s, consider using AddQueueNameSuffix", routingKey, mappedRoutingKey, queueName)
	}
	queueHandlers.add(routingKey, handler)
	return nil
}

type queueWithHandlers struct {
	Name     string
	Handlers *handlers
}

// queues returns all queue names for which we have added a handler
func (h *queueHandlers) queues() []queueWithHandlers {
	if h == nil {
		return []queueWithHandlers{}
	}
	var res []queueWithHandlers
	for q, h := range *h {
		res = append(res, queueWithHandlers{Name: q, Handlers: h})
	}
	return res
}

// handlers returns all the handlers for a given queue, keyed by the routing key
func (h *queueHandlers) handlers(queueName string) *handlers {
	if h == nil {
		return &handlers{}
	}

	if handlers, ok := (*h)[queueName]; ok {
		return handlers
	}
	return &handlers{}
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

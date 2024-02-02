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

package handlers

import (
	"fmt"
)

// Handler is the handler for a certain routing key and queue
type Handler any

// Handlers holds the handlers for a certain queue
type Handlers[T Handler] map[string]*T

// Get returns the handler for the given queue and routing key that matches
func (h *Handlers[T]) Get(routingKey string) (*T, bool) {
	for mappedRoutingKey, handler := range *h {
		if match(mappedRoutingKey, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

// exists returns the already mapped routing key if it exists (matched by the matches function to support wildcards)
func (h *Handlers[T]) exists(routingKey string) (string, bool) {
	for mappedRoutingKey := range *h {
		if overlaps(routingKey, mappedRoutingKey) {
			return mappedRoutingKey, true
		}
	}
	return "", false
}

func (h *Handlers[T]) add(routingKey string, handler *T) {
	(*h)[routingKey] = handler
}

// QueueHandlers holds all handlers for all queues
type QueueHandlers[T Handler] map[string]*Handlers[T]

// Add a handler for the given queue and routing key
func (h *QueueHandlers[T]) Add(queueName, routingKey string, handler *T) error {
	queueHandlers, ok := (*h)[queueName]
	if !ok {
		queueHandlers = &Handlers[T]{}
		(*h)[queueName] = queueHandlers
	}

	if mappedRoutingKey, exists := queueHandlers.exists(routingKey); exists {
		return fmt.Errorf("routingkey %s overlaps %s for queue %s, consider using AddQueueNameSuffix", routingKey, mappedRoutingKey, queueName)
	}
	queueHandlers.add(routingKey, handler)
	return nil
}

type Queue[T Handler] struct {
	Name     string
	Handlers *Handlers[T]
}

// Queues returns all queue names for which we have added a handler
func (h *QueueHandlers[T]) Queues() []Queue[T] {
	if h == nil {
		return []Queue[T]{}
	}
	var res []Queue[T]
	for q, h := range *h {
		res = append(res, Queue[T]{Name: q, Handlers: h})
	}
	return res
}

// Handlers returns all the handlers for a given queue, keyed by the routing key
func (h *QueueHandlers[T]) Handlers(queueName string) *Handlers[T] {
	if h == nil {
		return &Handlers[T]{}
	}

	if handlers, ok := (*h)[queueName]; ok {
		return handlers
	}
	return &Handlers[T]{}
}

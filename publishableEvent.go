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
	"time"

	"github.com/google/uuid"
)

// PublishableEvent represents an event that can be published.
// The Payload field holds the event's payload data, which can be of
// any type that can be marshal to json.
type PublishableEvent struct {
	Metadata
	Payload any
}

type eventOptions struct {
	eventID       string
	correlationID string
}

// WithEventID specifies the eventID to be published
// if it is not used a random uuid will be generated.
func WithEventID(eventID string) func(*eventOptions) {
	return func(opt *eventOptions) {
		opt.eventID = eventID
	}
}

// WithCorrelationID specifies the correlationID to be published
// if it is not used a random uuid will be generated.
func WithCorrelationID(correlationID string) func(*eventOptions) {
	return func(opt *eventOptions) {
		opt.correlationID = correlationID
	}
}

// NewPublishableEvent creates an instance of a PublishableEvent.
// In case the ID and correlation ID are not supplied via options random uuid will be generated.
func NewPublishableEvent(payload any, opts ...func(*eventOptions)) PublishableEvent {
	evtOpts := eventOptions{}
	for _, opt := range opts {
		opt(&evtOpts)
	}

	if evtOpts.correlationID == "" {
		evtOpts.correlationID = uuid.NewString()
	}
	if evtOpts.eventID == "" {
		evtOpts.eventID = uuid.NewString()
	}

	return PublishableEvent{
		Metadata: Metadata{
			ID:            evtOpts.eventID,
			CorrelationID: evtOpts.correlationID,
			Timestamp:     time.Now(),
		},
		Payload: payload,
	}
}

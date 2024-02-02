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
	"encoding/json"
	"time"
)

// Metadata holds the metadata of an event.
type Metadata struct {
	ID            string    `json:"id"`
	CorrelationID string    `json:"correlationId"`
	Timestamp     time.Time `json:"timestamp"`
}

// DeliveryInfo holds information of original queue, exchange and routing keys.
type DeliveryInfo struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Headers    Headers
}

// ConsumableEvent represents an event that can be consumed.
// The type parameter T specifies the type of the event's payload.
type ConsumableEvent[T any] struct {
	Metadata
	DeliveryInfo DeliveryInfo
	Payload      T
}

// unmarshalEvent is used internally to unmarshal a PublishableEvent
// this way the payload ends up being a json.RawMessage instead of map[string]interface{}
// so that later the json.RawMessage can be unmarshal to ConsumableEvent[T].Payload.
type unmarshalEvent struct {
	Metadata
	DeliveryInfo DeliveryInfo
	Payload      json.RawMessage
}

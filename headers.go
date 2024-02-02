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
	"fmt"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Header represent meta-data  for the message
// This is backed by an amqp.Table so the same restrictions regarding the type allowed for Value applies
type Header struct {
	Key   string
	Value any
}

// Headers represent all meta-data for the message
type Headers map[string]any

// Get returns the header value for key, or nil if not present
func (h Headers) Get(key string) any {
	if v, ok := h[key]; ok {
		return v
	}
	return nil
}

func (h Header) validateKey() error {
	if len(h.Key) == 0 || h.Key == "" {
		return errors.New("empty key not allowed")
	}
	for _, rh := range reservedHeaderKeys {
		if rh == h.Key {
			return fmt.Errorf("reserved key %s used, please change to use another one", rh)
		}
	}
	return nil
}

func (h Headers) validate() error {
	for k, v := range h {
		h := Header{k, v}
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	return nil
}

func headers(headers amqp.Table, routingKey string) Headers {
	if headers == nil {
		headers = make(amqp.Table)
	}
	headers["routing-key"] = routingKey
	return Headers(headers)
}

var reservedHeaderKeys = []string{headerService}

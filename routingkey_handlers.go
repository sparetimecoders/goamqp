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
	"fmt"
	"regexp"
	"strings"
)

// routingKeyHandler holds the mapping from routing key to a specific handler
type routingKeyHandler map[string]wrappedHandler

// get returns the handler for the given routing key that matches
func (h *routingKeyHandler) get(routingKey string) (wrappedHandler, bool) {
	for mappedRoutingKey, handler := range *h {
		if match(mappedRoutingKey, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

// exists returns the already mapped routing key if it exists (matched by the overlaps function to support wildcards)
func (h *routingKeyHandler) exists(routingKey string) (string, bool) {
	for mappedRoutingKey := range *h {
		if overlaps(routingKey, mappedRoutingKey) {
			return mappedRoutingKey, true
		}
	}
	return "", false
}

func (h *routingKeyHandler) add(routingKey string, handler wrappedHandler) {
	(*h)[routingKey] = handler
}

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

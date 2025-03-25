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
	"regexp"
	"strings"
)

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
	replace := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s, ".", "\\."), "*", "[^.]*"), "#", ".*")
	return fmt.Sprintf("^%s$", replace)
}

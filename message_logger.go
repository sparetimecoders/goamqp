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

package goamqp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

// MessageLogger is a func that can be used to log in/outgoing messages for debugging purposes
type MessageLogger func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool)

// NoOpMessageLogger is a MessageLogger that will do nothing
// This is the default implementation if the setup func UseMessageLogger is not used
func noOpMessageLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
	}
}

// StdOutMessageLogger is an example implementation of a MessageLogger that dumps messages with fmt.Printf
func StdOutMessageLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
		var prettyJSON bytes.Buffer
		err := json.Indent(&prettyJSON, jsonContent, "", "\t")
		var prettyJSONString string
		if err != nil {
			prettyJSONString = string(jsonContent)
		} else {
			prettyJSONString = prettyJSON.String()
		}
		if outgoing {
			fmt.Printf("Sending [%s] using routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJSONString)
		}
		fmt.Printf("Received [%s] from routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJSONString)
	}
}

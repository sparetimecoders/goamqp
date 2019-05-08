// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package goamqp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type missingProcess struct{}

type Message struct{}

func TestCheckHandlerMissingMethod(t *testing.T) {
	err := checkMessageHandler(&missingProcess{})
	fmt.Println(reflect.TypeOf(missingProcess{}))
	assert.EqualError(t, err, fmt.Sprintf("handler %s is missing Process method", reflect.TypeOf(missingProcess{})))
}

type incorrectProcessArgumentCount struct{}

func (incorrectProcessArgumentCount) Process() {}

func TestCheckHandlerIncorrectNumberOfArguments(t *testing.T) {
	err := checkMessageHandler(&incorrectProcessArgumentCount{})
	assert.EqualError(t, err, fmt.Sprintf("handler %s has incorrect number of arguments, expected 1 but was 0", reflect.TypeOf(incorrectProcessArgumentCount{})))
}

type missingReturnValue struct{}

func (missingReturnValue) Process(msg Message) {}

func TestCheckHandlerMissingReturnValue(t *testing.T) {
	err := checkMessageHandler(&missingReturnValue{})
	assert.EqualError(t, err, fmt.Sprintf("handler %s has incorrect number of return values. Expected 1, actual 0", reflect.TypeOf(missingReturnValue{})))
}

type OutMessage struct{}

type correctRequestResponseHandler struct{}

func (correctRequestResponseHandler) Process(msg Message) (OutMessage, bool) {
	return OutMessage{}, true
}

func TestCorrectCheckRequestResponseHandler(t *testing.T) {
	err := checkRequestResponseHandlerReturns(&correctRequestResponseHandler{})
	assert.NoError(t, err)
}

func TestCheckHandlerNotAPointer(t *testing.T) {
	err := checkMessageHandler(correctRequestResponseHandler{})
	assert.EqualError(t, err, fmt.Sprintf("handler %s is not a pointer", reflect.TypeOf(correctRequestResponseHandler{})))
}

type requestResponseMessageHandlerMissingReturn struct{}

func (requestResponseMessageHandlerMissingReturn) Process(msg Message) bool { return true }

func TestCheckHandlerRequestResponseMessageHandlerMissingReturn(t *testing.T) {
	err := checkRequestResponseHandlerReturns(&requestResponseMessageHandlerMissingReturn{})
	assert.EqualError(t, err, fmt.Sprintf("handler %s has incorrect number of return values. Expected 2, actual 1", reflect.TypeOf(requestResponseMessageHandlerMissingReturn{})))
}

type incorrectRequestResponseMessageHandler struct{}

func (incorrectRequestResponseMessageHandler) Process(msg Message) {}

func TestCheckHandlerIncorrectRequestResponseMessageHandler(t *testing.T) {
	err := checkMessageHandler(&incorrectRequestResponseMessageHandler{})
	assert.EqualError(t, err, fmt.Sprintf("handler %s has incorrect number of return values. Expected 1, actual 0", reflect.TypeOf(incorrectRequestResponseMessageHandler{})))
}

func TestNilHandlerNotAllowed(t *testing.T) {
	err := checkMessageHandler(nil)
	assert.EqualError(t, err, "handler is nil")

	err = checkRequestResponseHandlerReturns(nil)
	assert.EqualError(t, err, "handler is nil")
}

func TestInputType(t *testing.T) {
	x := inputType(correctRequestResponseHandler{})
	fmt.Println(x)
}

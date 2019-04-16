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

package go_amqp

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type missingProcess struct{}

func (missingProcess) Type() interface{} {
	return nil
}

type Message struct{}

func TestCheckHandlerMissingMethod(t *testing.T) {
	_, err := checkHandler(&missingProcess{})
	assert.EqualError(t, err, "missing method Process on handler, go_amqp.missingProcess")
}

type incorrectProcessArgumentCount struct{}

func (incorrectProcessArgumentCount) Process() {}

func (incorrectProcessArgumentCount) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectNumberOfArguments(t *testing.T) {
	_, err := checkHandler(&incorrectProcessArgumentCount{})
	assert.EqualError(t, err, "incorrect number of arguments, expected 1 but was 0")
}

type incorrectProcessInput struct{}

func (incorrectProcessInput) Process(msg string) {}

func (incorrectProcessInput) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectArguments(t *testing.T) {
	_, err := checkHandler(&incorrectProcessInput{})
	assert.EqualError(t, err, "incorrect in arguments. Expected Process(go_amqp.Message), actual Process(string)")
}

type missingReturnValue struct{}

func (missingReturnValue) Process(msg Message) {}

func (missingReturnValue) Type() interface{} { return Message{} }

func TestCheckHandlerMissingReturnValue(t *testing.T) {
	_, err := checkHandler(&missingReturnValue{})
	assert.EqualError(t, err, "incorrect number of return values. Expected 1, actual 0")
}

type incorrectProcessReturn struct{}

func (incorrectProcessReturn) Process(msg Message) string { return "" }

func (incorrectProcessReturn) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectReturn(t *testing.T) {
	_, err := checkHandler(&incorrectProcessReturn{})
	assert.EqualError(t, err, "incorrect return type for Process(go_amqp.Message). Expected bool, actual string")
}

type correctIncomingMessageHandler struct{}

func (correctIncomingMessageHandler) Process(msg Message) bool { return true }

func (correctIncomingMessageHandler) Type() interface{} { return Message{} }

func TestCheckHandlerCorrectIncomingMessageHandler(t *testing.T) {
	_, err := checkHandler(&correctIncomingMessageHandler{})
	assert.NoError(t, err)
}

func TestCheckHandlerNotAPointer(t *testing.T) {
	_, err := checkHandler(correctIncomingMessageHandler{})
	assert.Error(t, err, "handler is not a pointer")
}

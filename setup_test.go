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
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CloseListener(t *testing.T) {
	listener := make(chan error)
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := CloseListener(listener)(conn)
	require.NoError(t, err)
	require.Equal(t, true, channel.NotifyCloseCalled)
	// nil is ignored
	channel.ForceClose(nil)
	channel.ForceClose(&amqp.Error{Code: 123, Reason: "Close reason"})
	err = <-listener
	require.EqualError(t, err, "Exception (123) Reason: \"Close reason\"")
}

func Test_EventStreamPublisher_FailedToCreateExchange(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	p := NewPublisher()

	e := errors.New("failed to create exchange")
	channel.ExchangeDeclarationError = &e
	err := EventStreamPublisher(p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, "failed to declare exchange events.topic.exchange, failed to create exchange")
}

func Test_ServicePublisher_ExchangeDeclareFail(t *testing.T) {
	e := errors.New("failed")
	channel := NewMockAmqpChannel()
	channel.ExchangeDeclarationError = &e
	conn := mockConnection(channel)

	p := NewPublisher()

	err := ServicePublisher("svc", p)(conn)
	require.Error(t, err)
	require.EqualError(t, err, e.Error())
}

func Test_PublishNotify(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	notifier := make(chan amqp.Confirmation)
	err := PublishNotify(notifier)(conn)
	require.NoError(t, err)
	require.Equal(t, &notifier, channel.Confirms)
	require.Equal(t, true, channel.ConfirmCalled)
}

func Test_WithTypeMapping_KeyAlreadyExist(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := WithTypeMapping("key", TestMessage{})(conn)
	assert.NoError(t, err)
	err = WithTypeMapping("key", TestMessage2{})(conn)
	assert.EqualError(t, err, "mapping for routing key 'key' already registered to type 'goamqp.TestMessage'")
}

func Test_WithTypeMapping_TypeAlreadyExist(t *testing.T) {
	channel := NewMockAmqpChannel()
	conn := mockConnection(channel)
	err := WithTypeMapping("key", TestMessage{})(conn)
	assert.NoError(t, err)
	err = WithTypeMapping("other", TestMessage{})(conn)
	assert.EqualError(t, err, "mapping for type 'goamqp.TestMessage' already registered to routing key 'key'")
}

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

import "fmt"

type NotificationSource string

const (
	NotificationSourceConsumer NotificationSource = "CONSUMER"
)

type NotificationType string

const (
	NotificationTypeInfo  NotificationType = "INFO"
	NotificationTypeError NotificationType = "ERROR"
)

type Notification struct {
	Message string
	Type    NotificationType
	Source  NotificationSource
}

func notifyEventHandlerSucceed(ch chan<- Notification, routingKey string, took int64) {
	if ch != nil {
		ch <- Notification{
			Type:    NotificationTypeInfo,
			Message: fmt.Sprintf("event handler for %s succeeded, took %d milliseconds", routingKey, took),
			Source:  NotificationSourceConsumer,
		}
	}
}

func notifyEventHandlerFailed(ch chan<- Notification, routingKey string, took int64, err error) {
	if ch != nil {
		ch <- Notification{
			Type:    NotificationTypeError,
			Message: fmt.Sprintf("event handler for %s failed, took %d milliseconds, error: %s", routingKey, took, err),
			Source:  NotificationSourceConsumer,
		}
	}
}

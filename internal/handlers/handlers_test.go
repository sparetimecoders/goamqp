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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_QueueHandlers(t *testing.T) {
	qh := QueueHandlers[string]{}
	queues := []string{"q1", "q2", "q3"}
	keys := []string{"rk1", "rk2", "rk3"}
	for _, q := range queues {
		for _, rk := range keys {
			require.NoError(t, qh.Add(q, rk, ptr(fmt.Sprintf("%s.%s", q, rk))))
		}
	}

	require.Equal(t, len(queues), len(qh.Queues()))
	for _, q := range qh.Queues() {
		require.Equal(t, q.Handlers, qh.Handlers(q.Name))
	}

	handlerFor := func(q, r string) string {
		h, exists := qh.Handlers(q).Get(r)
		require.True(t, exists)
		return *h
	}
	for _, q := range queues {
		for _, rk := range keys {
			require.Equal(t, fmt.Sprintf("%s.%s", q, rk), handlerFor(q, rk))
		}
	}

	require.Equal(t, &Handlers[string]{}, qh.Handlers("missing"))
	_, exists := qh.Handlers(queues[0]).Get("missing")
	require.False(t, exists)
}

func Test_QueueHandlersOverlap(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		existing []string
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name:     "empty",
			key:      "a",
			existing: nil,
			wantErr:  assert.NoError,
		},
		{
			name:     "dot split words",
			key:      "testing",
			existing: []string{"test.#"},
			wantErr:  assert.NoError,
		},
		{
			name:     "overlap no wildcard",
			key:      "a",
			existing: []string{"a"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "routingkey a overlaps a for queue q, consider using AddQueueNameSuffix")
			},
		},
		{
			name:     "overlap * wildcard",
			key:      "user.updated",
			existing: []string{"user.*"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "routingkey user.updated overlaps user.* for queue q, consider using AddQueueNameSuffix")
			},
		},
		{
			name:     "overlap # wildcard",
			key:      "user.#",
			existing: []string{"user.a.updated"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "routingkey user.# overlaps user.a.updated for queue q, consider using AddQueueNameSuffix")
			},
		},
		{
			name:     "overlap other wildcard",
			key:      "user.#",
			existing: []string{"user.a.*"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.EqualError(t, err, "routingkey user.# overlaps user.a.* for queue q, consider using AddQueueNameSuffix")
			},
		},
	}
	for _, tt := range tests {
		queueName := "q"
		ptrBool := ptr(true)
		t.Run(tt.name, func(t *testing.T) {
			h := QueueHandlers[bool]{}
			for _, s := range tt.existing {
				require.NoError(t, h.Add(queueName, s, ptrBool))
			}
			if !tt.wantErr(t, h.Add(queueName, tt.key, ptrBool)) {
				return
			}
		})
	}
}

func ptr[T any](b T) *T {
	return &b
}

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
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RoutingKeyHandlers(t *testing.T) {
	type mapping struct {
		key     string
		handler wrappedHandler
	}

	tests := []struct {
		name            string
		mappings        []mapping
		expectedMatches []string
		invalid         bool
	}{
		{
			name: "exists",
			mappings: []mapping{{
				key: "key",
			}},
			expectedMatches: []string{"key"},
		},
		{
			name: "wildcard in key",
			mappings: []mapping{{
				key: "key.a",
			}},
			expectedMatches: []string{"key.#"},
		},
		{
			name: "wildcard in mapping",
			mappings: []mapping{{
				key: "key.#",
			}},
			expectedMatches: []string{"key.a"},
		},
		{
			name: "invalid wildcard",
			mappings: []mapping{{
				key: `[`,
			}},
			invalid: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rkh := make(routingKeyHandler)
			for _, m := range tt.mappings {
				rkh.add(m.key, m.handler)
			}
			for _, m := range tt.expectedMatches {
				matchedRoutingKey, ok := rkh.exists(m)
				require.Equal(t, ok, !tt.invalid)
				_, ok = rkh.get(matchedRoutingKey)
				require.Equal(t, ok, !tt.invalid)
			}

			_, ok := rkh.exists("missing")
			require.False(t, ok)
			_, ok = rkh.get("missing")
			require.False(t, ok)
		})
	}
}

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
)

func Test_Overlaps(t *testing.T) {
	tests := []struct {
		name        string
		keys        [2]string
		wantOverlap bool
	}{
		{
			name: "no overlap without wildcard",
			keys: [2]string{"a", "ab"},
		},
		{
			name:        "overlap same",
			keys:        [2]string{"a", "a"},
			wantOverlap: true,
		},
		{
			name:        "overlap * wildcard",
			keys:        [2]string{"user.updated", "user.*"},
			wantOverlap: true,
		},
		{
			name:        "overlap multiple wildcard",
			keys:        [2]string{"user.a.#", "user.#"},
			wantOverlap: true,
		},
		{
			name:        "overlap # wildcard",
			keys:        [2]string{"user.#", "user.a.updated"},
			wantOverlap: true,
		},
		{
			name:        "overlap other wildcard",
			keys:        [2]string{"user.#", "user.a.*"},
			wantOverlap: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantOverlap {
				assert.True(t, overlaps(tt.keys[0], tt.keys[1]))
			} else {
				assert.False(t, overlaps(tt.keys[0], tt.keys[1]))
			}
		})
	}
}

func Test_Match(t *testing.T) {
	tests := []struct {
		name    string
		matches []string
		misses  []string
		pattern string
	}{
		{
			name:    "Errors in pattern",
			misses:  []string{""},
			pattern: "[[",
		},
		{
			name:    "Match all",
			matches: []string{"", "user", "user.1.2.3", "1.2.user."},
			pattern: "#",
		},
		{
			name:    "Match one dot word",
			matches: []string{"user.1", "user."},
			misses:  []string{"user", "user.1.2"},
			pattern: "user.*",
		},
		{
			name:    "Match one word",
			matches: []string{"user", "user1"},
			misses:  []string{"user.1", "user1.2.3.4"},
			pattern: "user*",
		},
		{
			name:    "Match multiple words",
			matches: []string{"user", "user1", "user1.2.3.4.5"},
			misses:  []string{"use", "abc"},
			pattern: "user#",
		},
		{
			name:    "Match multiple words and dots",
			matches: []string{"users.1.test.2.abc.def", "user.1.test.2.abc", "user.1.2.2.4.5"},
			misses:  []string{"use", "abc", "users.1.test.3.abc"},
			pattern: "user#.1.*.2.#",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range tt.matches {
				if !match(tt.pattern, key) {
					assert.Fail(t, fmt.Sprintf("%s should match %s", key, tt.pattern))
				}
			}
			for _, key := range tt.misses {
				if match(tt.pattern, key) {
					assert.Fail(t, fmt.Sprintf("%s should NOT match %s", key, tt.pattern))
				}
			}
		})
	}
}

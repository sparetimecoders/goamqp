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
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Headers(t *testing.T) {
	h := Headers{}
	require.NoError(t, h.validate())

	h = Headers{"valid": ""}
	require.NoError(t, h.validate())
	require.Equal(t, "", h.Get("valid"))
	require.Nil(t, h.Get("invalid"))

	h = Headers{"valid1": "1", "valid2": "2"}
	require.Equal(t, "1", h.Get("valid1"))
	require.Equal(t, "2", h.Get("valid2"))

	h = map[string]any{headerService: "p"}
	require.EqualError(t, h.validate(), "reserved key service used, please change to use another one")

	h = map[string]any{"": "p"}
	require.EqualError(t, h.validate(), "empty key not allowed")

	h = Headers{headerService: "aService"}
	require.Equal(t, h.Get(headerService), "aService")
}

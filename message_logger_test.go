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
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStdoutLogger(t *testing.T) {
	file, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer func() { _ = os.Remove(file.Name()) }()
	logger := StdOutMessageLogger()
	stdout := os.Stdout
	os.Stdout = file
	logger([]byte(`{"key":"value"}`), reflect.TypeOf(Connection{}), "key", false)
	os.Stdout = stdout
	require.NoError(t, file.Close())
	all, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	require.Equal(t, "Received [goamqp.Connection] from routingkey: 'key' with content:\n{\n\t\"key\": \"value\"\n}\n", string(all))
}

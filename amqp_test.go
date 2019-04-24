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
	"gopkg.in/go-playground/validator.v9"
	"testing"
)

type testStruct struct {
	Message string `json:"msg"`
}

type testRequiredStruct struct {
	Message string `json:"msg" validate:"required"`
}

func TestParseValidJson(t *testing.T) {
	c := connection{validate: validator.New()}
	_, err := c.parseAndValidateJson([]byte("{}"), &testStruct{})
	assert.NoError(t, err)
}

func TestParseInValidJson(t *testing.T) {
	c := connection{validate: validator.New()}
	_, err := c.parseAndValidateJson([]byte("{"), &testStruct{})
	assert.EqualError(t, err, "unexpected end of JSON input")
}

func TestValidRequiredField(t *testing.T) {
	c := connection{validate: validator.New()}
	_, err := c.parseAndValidateJson([]byte(`{"msg":"1"}`), &testRequiredStruct{})
	assert.NoError(t, err)
}

func TestParseMissingRequiredField(t *testing.T) {
	c := connection{validate: validator.New()}
	_, err := c.parseAndValidateJson([]byte("{}"), &testRequiredStruct{})
	assert.EqualError(t, err, "Key: 'testRequiredStruct.Message' Error:Field validation for 'Message' failed on the 'required' tag")
}

func TestParseWrongFieldType(t *testing.T) {
	c := connection{validate: validator.New()}
	_, err := c.parseAndValidateJson([]byte(`{"msg":1}`), &testRequiredStruct{})
	assert.EqualError(t, err, "json: cannot unmarshal number into Go struct field testRequiredStruct.msg of type string")
}

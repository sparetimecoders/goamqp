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
	"github.com/caarlos0/env"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestParseValidUrl(t *testing.T) {
	c, err := ParseAmqpURL("amqp://localhost:user@password:67333/a")
	assert.NoError(t, err)
	assert.EqualValues(t, AmqpConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     67333,
		VHost:    "a",
	},
		c)
}
func TestParseValidUrlWithDefaults(t *testing.T) {
	c, err := ParseAmqpURL("localhost:user@password")
	assert.NoError(t, err)
	assert.EqualValues(t, AmqpConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     5672,
		VHost:    "",
	},
		c)
	assert.Equal(t, "amqp://user:password@localhost:5672/", c.AmqpURL())
}

func TestParseUrlMissingHost(t *testing.T) {
	_, err := ParseAmqpURL("amqp://:user@password:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing host from config")
}

func TestParseUrlMissingUsername(t *testing.T) {
	_, err := ParseAmqpURL("amqp://localhost:@password:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing username from config")
}

func TestParseUrlMissingPassword(t *testing.T) {
	_, err := ParseAmqpURL("amqp://localhost:user@:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing password from config")
}

func TestParseUrlDefaultPort(t *testing.T) {
	c, err := ParseAmqpURL("amqp://localhost:user@password/Vhost")
	assert.NoError(t, err)
	assert.Equal(t, "Vhost", c.VHost)
	assert.Equal(t, 5672, c.Port)
}

func TestParseUrlInvalid(t *testing.T) {
	_, err := ParseAmqpURL("amqp://localhost:user:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection url is invalid")
}

func TestEnvParse(t *testing.T) {
	_ = os.Setenv("RABBITMQ_HOST", "a")
	_ = os.Setenv("RABBITMQ_PORT", "1234")
	_ = os.Setenv("RABBITMQ_VHOST", "b")
	_ = os.Setenv("RABBITMQ_USERNAME", "c")
	_ = os.Setenv("RABBITMQ_PASSWORD", "d")
	_ = os.Setenv("RABBITMQ_DELAYED_MESSAGE_ENABLED", "false")

	c := &AmqpConfig{}
	err := env.Parse(c)
	assert.NoError(t, err)
	assert.Equal(t, "a", c.Host)
	assert.Equal(t, 1234, c.Port)
	assert.Equal(t, "b", c.VHost)
	assert.Equal(t, "c", c.Username)
	assert.Equal(t, "d", c.Password)
	assert.False(t, c.DelayedMessage)
}

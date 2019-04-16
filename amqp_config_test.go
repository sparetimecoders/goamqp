package go_amqp

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseValidUrl(t *testing.T) {
	c, err := ParseAmqpUrl("amqp://localhost:user@password:67333/a")
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
	c, err := ParseAmqpUrl("localhost:user@password")
	assert.NoError(t, err)
	assert.EqualValues(t, AmqpConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     5672,
		VHost:    "",
	},
		c)
	assert.Equal(t, "amqp://user:password@localhost:5672/", c.AmqpUrl())
}

func TestParseUrlMissingHost(t *testing.T) {
	_, err := ParseAmqpUrl("amqp://:user@password:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing host from config")
}

func TestParseUrlMissingUsername(t *testing.T) {
	_, err := ParseAmqpUrl("amqp://localhost:@password:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing username from config")
}

func TestParseUrlMissingPassword(t *testing.T) {
	_, err := ParseAmqpUrl("amqp://localhost:user@:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing password from config")
}

func TestParseUrlDefaultPort(t *testing.T) {
	c, err := ParseAmqpUrl("amqp://localhost:user@password/Vhost")
	assert.NoError(t, err)
	assert.Equal(t, "Vhost", c.VHost)
	assert.Equal(t, 5672, c.Port)
}

func TestParseUrlInvalid(t *testing.T) {
	_, err := ParseAmqpUrl("amqp://localhost:user:67333/a")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection url is invalid")
}

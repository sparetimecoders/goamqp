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
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// AmqpConfig contains the necessary variables for connecting to RabbitMQ.
type AmqpConfig struct {
	Username       string `env:"RABBITMQ_USERNAME,required"`
	Password       string `env:"RABBITMQ_PASSWORD,required"`
	Host           string `env:"RABBITMQ_HOST,required"`
	Port           int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	VHost          string `env:"RABBITMQ_VHOST" envDefault:""`
	DelayedMessage *bool  `env:"RABBITMQ_DELAYED_MESSAGED" envDefault:"true"`
}

// ParseAmqpURL tries to parse the passed string and create a valid AmqpConfig object
func ParseAmqpURL(amqpURL string) (AmqpConfig, error) {
	var amqpConnectionRegex = regexp.MustCompile(`(?:amqp:\/\/)?(?P<Host>.*):(?P<Username>.*)@(?P<Password>.*?)(?:\:(?P<Port>\d*))?(?:\/(?P<VHost>.*))?$`)
	if amqpConnectionRegex.MatchString(amqpURL) {
		return validateConfig(*convertToAmqpConfig(mapValues(amqpConnectionRegex, amqpConnectionRegex.FindStringSubmatch(amqpURL))))
	}
	return AmqpConfig{}, fmt.Errorf("connection url is invalid, %s", amqpURL)
}

// AmqpURL returns a valid connection url
func (c AmqpConfig) AmqpURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", c.Username, c.Password, c.Host, c.Port, c.VHost)
}

func convertToAmqpConfig(mappedValues map[string]string) *AmqpConfig {
	c := &AmqpConfig{}
	r := reflect.ValueOf(c).Elem()
	for i := 0; i < r.NumField(); i++ {
		field := r.Type().Field(i)
		value := mappedValues[field.Name]
		v := reflect.ValueOf(value)
		if field.Type.Kind() == reflect.Int {
			atoi, _ := strconv.Atoi(value)
			r.Field(i).Set(reflect.ValueOf(atoi))
		} else if field.Type.Kind() == reflect.String {
			r.Field(i).Set(v)
		}
	}
	return c
}

func mapValues(amqpConnectionRegex *regexp.Regexp, groups []string) map[string]string {
	mappedValues := make(map[string]string)
	for i, name := range amqpConnectionRegex.SubexpNames() {
		if i != 0 && name != "" {
			mappedValues[name] = groups[i]
		}
	}
	return mappedValues
}

func validateConfig(c AmqpConfig) (AmqpConfig, error) {
	if strings.TrimSpace(c.Password) == "" {
		return c, fmt.Errorf("missing password from config %s", c)
	}

	if strings.TrimSpace(c.Host) == "" {
		return c, fmt.Errorf("missing host from config %s", c)
	}

	if strings.TrimSpace(c.Username) == "" {
		return c, fmt.Errorf("missing username from config %s", c)
	}
	if c.Port == 0 {
		c.Port = 5672
	}
	return c, nil
}

func (c AmqpConfig) String() string {
	type config AmqpConfig
	copy := config(c)
	copy.Password = "********"
	return fmt.Sprintf("%+v", copy)
}

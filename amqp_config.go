package go_amqp

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// A Config that contains the necessary variables for connecting to RabbitMQ.
type AmqpConfig struct {
	Username string `env:"RABBITMQ_USERNAME,required"`
	Password string `env:"RABBITMQ_PASSWORD,required"`
	Host     string `env:"RABBITMQ_HOST,required"`
	Port     int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	VHost    string `env:"RABBITMQ_VHOST" envDefault:""`
}


func ParseAmqpUrl(amqpUrl string) (AmqpConfig, error) {
	var amqpConnectionRegex = regexp.MustCompile(`(?:amqp:\/\/)?(?P<Host>.*):(?P<Username>.*)@(?P<Password>.*?)(?:\:(?P<Port>\d*))?(?:\/(?P<VHost>.*))?$`)
	if amqpConnectionRegex.MatchString(amqpUrl) {
		return validateConfig(*convertToAmqpConfig(mapValues(amqpConnectionRegex, amqpConnectionRegex.FindStringSubmatch(amqpUrl))))
	} else {
		return AmqpConfig{}, errors.New(fmt.Sprintf("connection url is invalid, %s", amqpUrl))
	}
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
		return c, errors.New(fmt.Sprintf("missing password from config %s", c))
	}

	if strings.TrimSpace(c.Host) == "" {
		return c, errors.New(fmt.Sprintf("missing host from config %s", c))
	}

	if strings.TrimSpace(c.Username) == "" {
		return c, errors.New(fmt.Sprintf("missing username from config %s", c))
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

func (c AmqpConfig) AmqpUrl() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", c.Username, c.Password, c.Host, c.Port, c.VHost)
}

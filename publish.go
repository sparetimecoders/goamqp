package goamqp

import (
	"fmt"
	"reflect"

	"github.com/streadway/amqp"
)

// Publisher is used to send messages
type Publisher struct {
	connection       *Connection
	exchange         string
	defaultHeaders   []Header
	typeToRoutingKey routes
}

// NewPublisher returns a publisher that can be used to send messages
func NewPublisher(routes ...Route) *Publisher {
	r := make(map[reflect.Type]string)
	for _, route := range routes {
		r[reflect.TypeOf(route.Type)] = route.Key
	}

	return &Publisher{
		typeToRoutingKey: r,
	}
}

// Publish publishes a message to a given exchange
// TODO Document how messages flow, reference docs.md?
func (p *Publisher) Publish(msg interface{}, headers ...Header) error {
	table := amqp.Table{}
	for _, v := range p.defaultHeaders {
		table[v.Key] = v.Value
	}
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
		table[h.Key] = h.Value
	}

	t := reflect.TypeOf(msg)
	key := t
	if t.Kind() == reflect.Ptr {
		key = t.Elem()
	}
	if key, ok := p.typeToRoutingKey[key]; ok {
		return p.connection.publishMessage(msg, key, p.exchange, table)
	}
	return fmt.Errorf("no routingkey configured for message of type %s", t)
}

func (p *Publisher) setDefaultHeaders(serviceName string, headers ...Header) error {
	for _, h := range headers {
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	p.defaultHeaders = append(headers, Header{Key: headerService, Value: serviceName})
	return nil
}

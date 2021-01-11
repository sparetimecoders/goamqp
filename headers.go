package goamqp

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Header represent meta-data  for the message
// This is backed by an amqp.Table so the same restrictions regarding the type allowed for Value applies
type Header struct {
	Key   string
	Value interface{}
}

// Headers represent all meta-data for the message
type Headers struct {
	headers map[string]interface{}
}

// Get returns the header value for key, or nil if not present
func (h Headers) Get(key string) interface{} {
	if v, ok := h.headers[key]; ok {
		return v
	}
	return nil
}

func (h Header) validateKey() error {
	if len(h.Key) == 0 || h.Key == "" {
		return errors.New("empty key not allowed")
	}
	for _, rh := range reservedHeaderKeys {
		if rh == h.Key {
			return fmt.Errorf("reserved key %s used, please change to use another one", rh)
		}
	}
	return nil
}

func (h Headers) validate() error {
	for k, v := range h.headers {
		h := Header{k, v}
		if err := h.validateKey(); err != nil {
			return err
		}
	}
	return nil
}

func headers(headers amqp.Table) Headers {
	return Headers{headers: headers}
}

var reservedHeaderKeys = []string{headerService}

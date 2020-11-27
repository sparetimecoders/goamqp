package goamqp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

// MessageLogger is a func that can be used to log in/outgoing messages for debugging purposes
type MessageLogger func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool)

// NopLogger is a MessageLogger that will do nothing
// This is the default implementation if the setup func UseMessageLogger is not used
func NopLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
	}
}

// StdOutMessageLogger is an example implementation of a MessageLogger that dumps messages with fmt.Printf
func StdOutMessageLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
		var prettyJSON bytes.Buffer
		err := json.Indent(&prettyJSON, jsonContent, "", "\t")
		var prettyJSONString string
		if err != nil {
			prettyJSONString = string(jsonContent)
		} else {
			prettyJSONString = string(prettyJSON.Bytes())
		}
		if outgoing {
			fmt.Printf("Sending [%s] using routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJSONString)
		}
		fmt.Printf("Received [%s] from routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJSONString)
	}
}

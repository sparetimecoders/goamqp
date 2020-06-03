package goamqp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

type MessageLogger func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool)

func NopLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
	}
}

func StdOutMessageLogger() MessageLogger {
	return func(jsonContent []byte, eventType reflect.Type, routingKey string, outgoing bool) {
		var prettyJson bytes.Buffer
		err := json.Indent(&prettyJson, jsonContent, "", "\t")
		var prettyJsonString string
		if err != nil {
			prettyJsonString = string(jsonContent)
		} else {
			prettyJsonString = string(prettyJson.Bytes())
		}
		if outgoing {
			fmt.Printf("Sending [%s] using routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJsonString)
		} else {
			fmt.Printf("Received [%s] from routingkey: '%s' with content:\n%s\n", eventType, routingKey, prettyJsonString)
		}
	}
}

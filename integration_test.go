// +build integration

package goamqp

import (
	"github.com/stretchr/testify/assert"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	dialAmqp = dialConfig

	server, err := NewFromURL("server", "amqp://user:password@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	err = server.Start(
		RequestResponseHandler(
			"key",
			func(msg interface{}) (interface{}, bool) {
				assert.Equal(t, &Incoming{Query: "test"}, msg.(*Incoming))
				return IncomingResponse{Value: "Value",}, true
			}, reflect.TypeOf(Incoming{})))
	if err != nil {
		log.Fatal(err)
	}

	defer server.Close()

	publish := make(chan interface{})
	closer := make(chan bool)
	client, err := NewFromURL("client", "amqp://user:password@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Start(
		ServicePublisher("server", "key", publish, func(i interface{}) bool {
			assert.Equal(t, &IncomingResponse{Value: "Value"}, i.(*IncomingResponse))
			closer <- true
			return true
		}, reflect.TypeOf(IncomingResponse{})))
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	publish <- Incoming{Query: "test"}

	go forceClose(closer)
	<-closer
}

func forceClose(closer chan bool) {
	time.Sleep(10 * time.Second)
	closer <- true

}

type Incoming struct {
	Query string
}

type IncomingResponse struct {
	Value string
}

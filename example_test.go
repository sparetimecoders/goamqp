package go_amqp_test

import (
	"fmt"
	"gitlab.com/sparetimecoders/go_amqp"
	"log"
	"math/rand"
	"strings"
	"time"
)

func (p IncomingMessage) Process() bool {
	fmt.Println(p.Url)
	if strings.Contains(p.Url, "OK") {
		return true
	}
	return false
}

type IncomingMessage struct {
	Url string
}

func (IncomingMessage) TTL() time.Duration {
	return time.Minute
}

func Example() {

	config := go_amqp.Config{
		AmqpConfig: go_amqp.AmqpConfig{

			Host:     "localhost",
			Port:     5672,
			Username: "admin",
			Password: "password",
			VHost:    "",
		},
		DelayedMessageSupported: true,
	}
	connection, err := go_amqp.New(config)
	if err != nil {
		log.Fatalln("failed to connect", err)
	}
	err = connection.NewEventStreamListener("test-service", "testkey", &TestIncomingMessageHandler{})
	if err != nil {
		log.Fatalln("failed to create listener", err)
	}
	p, err := connection.NewEventStreamPublisher("testkey")
	fmt.Println(err)
	if err != nil {
		log.Fatalln("failed to create publisher", err)
	}

	r := rand.New(rand.NewSource(99))
	for {
		fmt.Println("Sleep")
		time.Sleep(2 * time.Second)
		fmt.Println("Sending")

		if r.Int()%2 == 0 {
			p <- IncomingMessage{"FAILED"}
		} else {
			p <- IncomingMessage{"OK"}
		}
	}

}

type TestIncomingMessageHandler struct {
	ctx string
}

func (TestIncomingMessageHandler) Type() interface{} {
	return IncomingMessage{}
}

func (i TestIncomingMessageHandler) Process(m IncomingMessage) bool {
	fmt.Printf("Called process with %v and ctx %v\n", m, i.ctx)
	return true
}

package go_amqp_test

import (
	"fmt"
	"gitlab.com/sparetimecoders/go_amqp"
	"log"
	"math/rand"
	"strings"
	"time"
)

func (p IncomingMessage) Handle() bool {
	fmt.Println(p.Url)
	if strings.Contains(p.Url, "OK") {
		return true
	}
	return false
}

type IncomingMessage struct {
	Url string
}

func main() {
	config := go_amqp.Config{
		Host:     "localhost",
		Port:     5672,
		Username: "admin",
		Password: "password",
		VHost:    "",
	}
	connection, err := go_amqp.Connect(config)
	if err != nil {
		log.Fatalln("failed to connect", err)
	}

	go_amqp.NewEventStreamListener("test-service", "testkey", IncomingMessage{}, connection)

	p := go_amqp.NewEventStreamPublisher("testkey", connection)

	r := rand.New(rand.NewSource(99))
	for {
		fmt.Println("Sleep")
		time.Sleep(2 * time.Second)
		if r.Int()%2 == 0 {
			p <- IncomingMessage{"FAILED"}
		} else {
			p <- IncomingMessage{"OK"}
		}
	}
}

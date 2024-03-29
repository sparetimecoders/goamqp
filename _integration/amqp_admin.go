// MIT License
//
// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package _integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type amqpAdmin struct {
	httpClient   *http.Client
	amqpAdminURL string
	password     string
	username     string
	vhost        string
}

func AmqpAdmin(host string, port int, username, password, vhost string) *amqpAdmin {
	return &amqpAdmin{
		httpClient:   &http.Client{},
		amqpAdminURL: fmt.Sprintf("http://%s:%d/api", host, port),
		username:     username,
		password:     password,
		vhost:        vhost,
	}
}

func (a *amqpAdmin) CreateVHost() error {
	_, err := a.request(http.MethodPut, fmt.Sprintf("/vhosts/%s", a.vhost), nil)
	return err
}

func (a *amqpAdmin) DeleteVHost() error {
	_, err := a.request(http.MethodDelete, fmt.Sprintf("/vhosts/%s", a.vhost), nil)
	return err
}

func (a *amqpAdmin) GetExchange(name string) (*Exchange, error) {
	exchanges, err := a.GetExchanges(false)
	if err != nil {
		return nil, err
	}
	if found, ok := find(name, exchanges); ok {
		return &found, nil
	}
	return nil, fmt.Errorf("exchange not found")
}

func (a *amqpAdmin) GetExchanges(filterDefaults bool) ([]Exchange, error) {
	resp, err := a.request(http.MethodGet, fmt.Sprintf("/exchanges/%s", a.vhost), nil)
	if err != nil {
		return nil, err
	}
	var exchanges []Exchange
	err = json.NewDecoder(resp.Body).Decode(&exchanges)
	if filterDefaults {
		var filtered []Exchange
		for _, e := range exchanges {
			if !defaultExchange(e.Name) {
				filtered = append(filtered, e)
			}
		}
		return filtered, nil
	}
	return exchanges, err
}

func (a *amqpAdmin) GetQueue(name string) (*Queue, error) {
	queues, err := a.GetQueues()
	if err != nil {
		return nil, err
	}
	if found, ok := find(name, queues); ok {
		return &found, nil
	}
	return nil, fmt.Errorf("queue %s not found", name)
}

func (a *amqpAdmin) GetQueues() ([]Queue, error) {
	resp, err := a.request(http.MethodGet, fmt.Sprintf("/queues/%s", a.vhost), nil)
	if err != nil {
		return nil, err
	}
	var queues []Queue
	err = json.NewDecoder(resp.Body).Decode(&queues)
	return queues, err
}

func (a *amqpAdmin) GetBindings(queueName string, filterDefault bool) ([]Binding, error) {
	resp, err := a.request(http.MethodGet, fmt.Sprintf("/queues/%s/%s/bindings", a.vhost, queueName), nil)
	if err != nil {
		return nil, err
	}
	var bindings []Binding
	err = json.NewDecoder(resp.Body).Decode(&bindings)
	if filterDefault {
		var filtered []Binding
		for _, b := range bindings {
			if b.Source != "" {
				filtered = append(filtered, b)
			}
		}
		return filtered, nil
	}
	return bindings, err
}

func (a *amqpAdmin) request(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", a.amqpAdminURL, path), body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(a.username, a.password)
	return a.httpClient.Do(req)
}

func defaultExchange(name string) bool {
	for _, e := range defaultExchanges {
		if e == name {
			return true
		}
	}
	return false
}

type Named interface {
	Named() string
}

func find[N Named](name string, names []N) (n N, found bool) {
	for _, n := range names {
		if n.Named() == name {
			return n, true
		}
	}
	return
}

type Exchange struct {
	AutoDelete bool   `json:"auto_delete"`
	Durable    bool   `json:"durable"`
	Internal   bool   `json:"internal"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Vhost      string `json:"vhost"`
}

func (e Exchange) Named() string {
	return e.Name
}

type QueueArguments struct {
	XQueueType string `json:"x-queue-type"`
	XExpires   int    `json:"x-expires"`
}
type Queue struct {
	Arguments            QueueArguments `json:"arguments"`
	AutoDelete           bool           `json:"auto_delete"`
	Durable              bool           `json:"durable"`
	Exclusive            bool           `json:"exclusive"`
	ExclusiveConsumerTag any            `json:"exclusive_consumer_tag"`
	Name                 string         `json:"name"`
	Vhost                string         `json:"vhost"`
}

type Binding struct {
	Source          string   `json:"source"`
	Vhost           string   `json:"vhost"`
	Destination     string   `json:"destination"`
	DestinationType string   `json:"destination_type"`
	RoutingKey      string   `json:"routing_key"`
	Arguments       struct{} `json:"arguments"`
}

func (q Queue) Named() string {
	return q.Name
}

var defaultExchanges = []string{"", "amq.direct", "amq.fanout", "amq.headers", "amq.match", "amq.rabbitmq.trace", "amq.topic"}

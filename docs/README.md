# Docs

Goamqp provides an opiniated way of using [rabbitmq](https://www.rabbitmq.com/) (and
[AMQP 0.9.1](https://www.rabbitmq.com/protocol.html) to be more precise) for event-driven architectures.
It defines a standard way of creating entities such as `exchanges`, `queues` and `bindings` with naming conventions and
support for structured messages in [JSON](https://www.json.org/json-en.html) format (marshalling to and from JSON is
handled automatically).

AMQP is a programmable protocol in that sense that entities and routing schemes are defined by the clients and not the
message broker (the rabbitmq server in this case). This means that for everything to work, all clients must define the
entities in the same way (i.e. they must be conformant).
This means that other packages (and for that matter, other programming languages) can be used together with `goamqp`
as long as they set up the entities in a conformant (and opiniated) way.

## AMQP

To get a brief introduction to AMQP, take a look at the articles
[concepts](https://www.rabbitmq.com/tutorials/amqp-concepts.html) and
[tutorial](https://www.rabbitmq.com/getstarted.html) that explains the basics of `exchanges`, `bindings` and `queues`.

In short:

* a message is published to an `exchange` and based on the `bindings` it will be delivered to zero, one or many `queues`.
* a consumer process messages when they arrive at a specific `queue`

## Basic example

Let's create a simple program to show the basic concepts.

### Setup

The code snippet below shows how to connect to a rabbitmq server, disregarding any errors and actually not doing
anything.

```go
conn, _ := goamqp.NewFromURL("our-service", "amqp://user:password@localhost:5672/")
_ = conn.Start(context.Background())
_ = conn.Close()
```

To actually create `exchanges`, `queues` and `bindings` we must pass one or more `Setup` funcs to the `Start` method.

### Publishing

The `Publisher` is used to send a messages (events) of a certain types.

```go
type AccountCreated struct {
    Name string `json:"name"`
}

publisher := goamqp.NewPublisher()
conn.Start(
  context.Background(),
  goamqp.WithTypeMapping("Account.Created", AccountCreated{}),
)

```
This will create a `Publisher` that will publish `AccountCreated` messages with an associated routing key
`Account.Created`. The same `Publisher` can be used to publish multiple message types for different routing keys.
In order for the `Publisher` to actually send any messages we must connect it to an `event stream`.

### Event streams
An event stream is basically an `topic exchange` that publishers can send messages to, from here they will be routed to
different queues depending on the `bindings`.
In this simple case we send messages to the default `event stream` by using the `Setup` func
`EventStreamPublisher`.

```go
conn.Start(
  context.Background(),
  goamqp.WithTypeMapping("Account.Created", AccountCreated{}),
  goamqp.EventStreamPublisher(publisher))
```

Now, when we call `Start` entities will be created on the message broker. A new exchange  called `events.topic.exchange`
will be created (if it doesn't already exist). Now when we do:

```go
publisher.Publish(&AccountCreated{Name: "test"})
```
the `AccountCreated` struct will be marshalled into JSON:

```json
{
  "name": "test"
}
```
and sent to the `events.topic.exchange` exchange. Since no one has subscribed (by setting up a binding to a queue) the
message is simply dropped!

Let's create a consumer as well.

### Consuming messages

Let's consume messages (in the same service, i.e. we send a message to ourselves) from the default `event stream` by
using the `Setup` func `EventStreamConsumer`.

```go
conn.Start(
  context.Background(),
  goamqp.EventStreamPublisher(publisher),
  goamqp.EventStreamConsumer("Account.Created", func(ctx context.Context, event goamqp.ConsumableEvent[AccountCreated]) error {
    fmt.Println("Message received")
    return nil
  }))

```

Now, when we call `Start` additional entities will be created on the message broker.
The exchange `events.topic.exchange`, a queue `events.topic.exchange.queue.our-service` and a binding from the
`events.topic.exchange` exchange with routing key `Account.Created` to that queue.
On the service side a consumer is created that will forward messages arriving to the queue
`events.topic.exchange.queue.our-service` to the `HandlerFunc` defined above which will just print `Message received`.

### Summary

The complete simple program below will send (and receive) a message and print it to the console:

```
Message received &{test}
```

```go
package main

import (
  "context"
  "fmt"
  "time"

  "github.com/sparetimecoders/goamqp"
)

type AccountCreated struct {
  Name string `json:"name"`
}

func main() {

  publisher := goamqp.NewPublisher()

  conn := goamqp.Must(goamqp.NewFromURL("our-service", "amqp://user:password@localhost:5672/"))

  _ = conn.Start(
    context.Background(),
    goamqp.EventStreamPublisher(publisher),
    goamqp.WithTypeMapping("Account.Created", AccountCreated{}),
    goamqp.EventStreamConsumer("Account.Created", func(ctx context.Context, event goamqp.ConsumableEvent[AccountCreated]) error {
      fmt.Printf("Message received %s", event.Payload.Name)
      return nil
    }))

  _ = publisher.Publish(context.Background(), &AccountCreated{Name: "test"})

  time.Sleep(time.Second)
  _ = conn.Close()
}
```

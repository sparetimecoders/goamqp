# Event stream

To publish events to the default event stream we need to create a `EventStreamPublisher` with a `Publisher` which maps types
and routing keys.

Let's start with the `Publisher`

```go
orderPublisher := NewPublisher()
```

The created `orderPublisher` can now be used to publish both `OrderCreated` and `OrderCreated` for different routing
keys when we start.

```go
orderServiceConnection.Start(
    EventStreamPublisher(orderPublisher),
    WithTypeMapping("Order.Created", OrderCreated{}),
    WithTypeMapping("Order.Updated", OrderUpdated{}),
)
```

No we can publish events:

```go
orderPublisher.Publish(context.Background(), OrderCreated{Id: "id"})
orderPublisher.Publish(context.Background(), OrderUpdated{Id: "id", Data: "data"})
```

Since no one is consuming the events they will of course just be dropped. Let's set up some consumers as well

The Stat service is only interested in created orders, so we just consume those events:
```go
connection = Must(NewFromURL("stat-service", amqpURL))
connection.Start(ctx,
    EventStreamConsumer("Order.Created", s.handleOrderCreated),
)

...

func (s *StatService) handleOrderCreated(ctx context.Context, msg ConsumableEvent[OrderCreated]) error {
    fmt.Printf("Created order: %s", msg.Payload.Id)
    return nil
}
```

The Shipping service is interested in all events for orders:
```go
connection.Start(ctx,
    WithTypeMapping("Order.Created", OrderCreated{}),
    WithTypeMapping("Order.Updated", OrderUpdated{}),
    EventStreamConsumer("#", TypeMappingHandler(func(ctx context.Context, event ConsumableEvent[any]) error {
      switch event.Payload.(type) {
      case *OrderCreated:
        s.output = append(s.output, "Order created")
      case *OrderUpdated:
        s.output = append(s.output, "Order deleted")
      }
        return nil
    }),
)
...

func handleOrderEvent(msg any, headers Headers) (response any, err error) {
    switch msg.(type) {
    case *OrderCreated:
        fmt.Println("Order created")
    case *OrderUpdated:
        fmt.Println("Order deleted")
    default:
        fmt.Println("Unknown message type")
    }
    return nil, nil
}
```
For both the stat- and shipping-service we define `HandlerFunc`s that process the incoming messages.

## AMQP
A number of queues, bindings and exchanges are now created to allow the events to flow from the publisher to the consumers.
The publisher publish a message to the exchange `events.topic.exchange`, this is the default event exchange and from the
naming convention it is clear that it is a topic exchange.

For the `events.topic.exchange` multiple bindings for `routingKey` is created to the different consumer queues.
The shipping-service will consume messages from the queue `events.topic.exchange.queue.shipping-service`, and the
stat-service from `events.topic.exchange.queue.stat-service`.
Notice the naming convention: `<exchangename>.queue.<consumername>`.

Since the Shippping-service is interested in both `Order.Created` and `Order.Updated` events two bindings are created,
one for each routing key.
And the Stat-service is interested in only `Order.Created` events so a single binding is created.

```mermaid
flowchart LR
    A((order)) --> B[events.topic.exchange]
    B --> C{Binding routingKey};
    C -- Order.Created --> D([events.topic.exchange.queue.shipping-service])
    C -- Order.Updated --> D([events.topic.exchange.queue.shipping-service])
    C -- Order.Created --> E([events.topic.exchange.queue.stat-service])
```

See a full example in [example_test.go](./example_test.go)

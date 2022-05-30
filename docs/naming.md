## Naming conventions for AMQP entities

**WIP**

Each service connecting to rabbitmq will need to pass a `service-name` that is used to identify the publisher and
consumer towards rabbitmq entities.
If multiple connections with the same name is used (for example when scaling to multiple replicas) all connections will
consume from the same queues
(i.e. [Competing consumers](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html)).

### Exchanges

The naming pattern for exchanges are: `<name>.<type>.exchange`, where name usually is the `service-name`.

#### Default event stream

The default event stream is named `events.topic.exchange`, which is an exception the pattern above.

#### Request-response

A service that is listening for incoming request (and send replies),
[request-response](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html) pattern will
create two exchanges. One to allow client to send a request, and one to send replies to.

The request exchange will be named: `<service-name>.direct.exchange.request`
and the corresponding response exchange: `<service-name>.headers.exchange.response`

### Queues
The naming pattern for queues are: `<exchange-name>.queue.<service-name>`

#### Default event stream

So for the default event stream `events.topic.exchange` the queues will be named:
`events.topic.exchange.queue.<service-name>`.

#### Request-response

A service that is listening for incoming *request* will consume messages from a queue:
`<exchange-name>.queue`, so it will not contain the service-name twice.

`<service-name>.direct.exchange.request.queue`

A service that is listening for incoming *responses* will consume messages from a queue:
`<requested-service-name>.headers.exchange.response.queue.<service-name>`


## References

For full reference take a look at the [code](../naming.go) and [tests](../naming_test.go)


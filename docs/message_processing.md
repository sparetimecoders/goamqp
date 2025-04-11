## Message processing and error handling

A registered `EventHandler` is called by goamqp when an event arrives on a queue.

```go
EventHandler[T any] func (ctx context.Context, event ConsumableEvent[T]) error
```

For most purposes a handler is just interested in the `event.Payload`.
You register a handler using one of the `..Consumer` functions, for example:

```go
goamqp.EventStreamConsumer("Order.Created", func(ctx context.Context, event goamqp.ConsumableEvent[Message]) error {
  fmt.Printf("handled %s", event.Payload.Text)
  return nil
})
```

For request-response, use the `RequestResponseEventHandler` and register it with the `RequestResponseHandler`:

```go
RequestResponseHandler[T any, R any](routingKey string, handler RequestResponseEventHandler[T, R])
```

```go
goamqp.RequestResponseHandler("req.resp", func (ctx context.Context, event goamqp.ConsumableEvent[Request]) (Response, error) {
  return Response{Output: event.Payload.Input}, nil
})
```

### Errors

If anything but `nil` is returned from `EventHandler` the event is considered Not Acknowledge and re-queued (which means
that it will be processed again).

If unmarshal the JSON payload in the event, the event will be rejected but **not** re-queued again.

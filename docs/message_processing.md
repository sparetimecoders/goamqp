## Message processing and error handling

The `HandlerFunc` is called by goamqp when a message arrive in a queue.

```go
type HandlerFunc func(msg any, headers Headers) (response any, err error)
```

For most purposes an application is only interested in the `msg` parameter, which will be our consumed message. Most
implementations will look like this:

```go
func handler(msg any, headers Headers) (response any, err error) {
    switch msg.(type) {
	case *Message:
	default:
		fmt.Println("Unknown message type")
	}
	return nil, nil
```

The `msg` will be a pointer to the type specified when calling `EventStreamConsumer`, for example:
```go
EventStreamConsumer("Order.Created", handler, Message{})
```

For normal event processing the returned `response` is ignored.
The same `HandlerFunc` is used for request-response handlers however, for example:

```go
RequestResponseHandler(routingKey, handleRequest, Request{})

func handleRequest(msg any, headers Headers) (response any, err error) {
    return Response{}}, nil
```

And in this case the returned `response` (`Response{}` in the code above) will be returned to the calling service.

Returning `nil` as error will Acknowledge the message and it will be removed from the queue.

### Errors

If anything but `nil` is returned from `HandlerFunc` the message will be rejected and requeued (which means that it will
be processed again).

If goamqp fails to unmarshal the JSON content in the message, the message will be rejected and **not** requeued again.

package goamqp

import "context"

// RequestResponseHandler is a convenience func to set up ServiceRequestConsumer and combines it with
// PublishServiceResponse
func RequestResponseHandler[T any, R any](routingKey string, handler RequestResponseEventHandler[T, R]) Setup {
	return func(c *Connection) error {
		responseHandlerWrapper := responseWrapper(handler, routingKey, func(ctx context.Context, targetService, routingKey string, msg R) error {
			return c.PublishServiceResponse(ctx, targetService, routingKey, msg)
		})
		return ServiceRequestConsumer[T](routingKey, responseHandlerWrapper)(c)
	}
}

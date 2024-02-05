package goamqp

func EventReceived(queue string, routingKey string) {
}

func EventWithoutHandler(queue string, routingKey string) {
}

func EventNotParsable(queue string, routingKey string) {
}

func EventNack(queue string, routingKey string, milliseconds int64) {
}

func EventAck(queue string, routingKey string, milliseconds int64) {
}

func EventPublishSucceed(exchange string, routingKey string) {
}

func EventPublishFailed(exchange string, routingKey string) {
}

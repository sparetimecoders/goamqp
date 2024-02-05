package goamqp

type Notification struct {
	Message string
	// Type    NotificationType
	// Source  NotificationSource
}

func notifyEventHandlerSucceed(ch chan<- Notification, routingKey string, took int64) {
}

func notifyEventHandlerFailed(ch chan<- Notification, routingKey string, took int64, err error) {
}

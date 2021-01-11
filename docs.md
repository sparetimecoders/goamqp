# Docs




Service A
* Listens to incoming requests on a queue service-a-request
* It can publish responses on an exchange service-a-response

Incoming requests contains a header (link to amqp doc) `service` which indicates where to send the reply.

Service send requests to an exchange:
<service-name>.direct.exchange.request

A binding is created from the exchange to a queue that can be processed by the service
<service-name>.direct.exchange.request.queue

serviceB servicePublisher(serviceA) -> serviceA.direct.exchange.request >=bind=> serviceA.direct.exchange.request.queue -> serviceA RequestListener

serviceA responsePublisher() -> delayer.headers.exchange.response >=bind=> serviceB.headers.exchange.response -> serviceB ResponseListener




Service B
ServiceListener


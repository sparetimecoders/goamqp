# go_amqp

[![GoReportCard](https://goreportcard.com/badge/gitlab.com/sparetimecoders/go_amqp)](https://goreportcard.com/report/gitlab.com/sparetimecoders/go_amqp) [![GoDoc](https://godoc.org/gitlab.com/sparetimecoders/go_amqp?status.svg)](https://godoc.org/gitlab.com/sparetimecoders/go_amqp) [![Build Status](https://gitlab.com/sparetimecoders/go_amqp/badges/master/pipeline.svg)](https://gitlab.com/sparetimecoders/go_amqp/commits/master)[![coverage report](https://gitlab.com/sparetimecoders/go_amqp/badges/master/coverage.svg)](https://gitlab.com/sparetimecoders/go_amqp/commits/master)

package goamqp provides an opiniated way of using [rabbitmq](https://www.rabbitmq.com/) for event-driven architectures.




Getting Started
===============

Supports Go 1.11+ and uses [streadway-amqp](https://github.com/streadway/amqp) to connect to RabbitMQ.

Using Go Modules
----------------

Starting with Go 1.13, you can use [Go Modules](https://blog.golang.org/using-go-modules) to install
goamqp.

Import the `goamqp` package from GitHub in your code:

```golang
import "gitlab.com/sparetimecoders/goamqp"
```

Build your project:

```bash
go build ./...
```

A dependency to the latest stable version of goamqp should be automatically added to
your `go.mod` file.

Install the client
------------------

If Go modules can't be used:

```bash
go get gitlab.com/sparetimecoders/go_amqp
```

## Usage

See the 'examples' subdirectory.

## Contributing
TODO


## References
* Official ampq [documentation](https://www.rabbitmq.com/tutorials/amqp-concepts.html)

## License
MIT - see [LICENSE](./LICENSE) for more details.


## Developing
TODO
## Tests

```bash
go test ./...
```

## Integration testing
Requires a running rabbitmq, for example:

```bash
docker run --name rabbit -p 15672:15672 -p 5672:5672 sparetimecoders/rabbitmq
```
Run the tests:
```bash
go test ./... -tags=integration
```

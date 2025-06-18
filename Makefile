.DEFAULT_GOAL := list

# Insert a comment starting with '##' after a target, and it will be printed by 'make' and 'make list'
.PHONY: list
list: ## list Makefile targets
	@echo "The most used targets: \n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## Run go fmt against code
	go fmt ./...

.PHONY: check copyright header
copyright: ## Check that source files have a copyright header
	@go install github.com/google/addlicense@v1.0.0
	@addlicense -f LICENSE -c "sparetimecoders" -skip yaml -skip yml -skip js -y "2019" --check .

.PHONY: check licenses
licenses: ## Check that source files have a copyright header
	@go install github.com/google/go-licenses@latest
	@go-licenses check .


.PHONY: tests
tests: ## Run all tests and requires a running rabbitmq-server
	go test -p 1 -mod=readonly -race -v -tags integration -coverprofile=coverage.txt -covermode=atomic -coverpkg=$(go list ./... | tr '\n' , | sed 's/,$//') ./...
	go tool cover -func=coverage.txt

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: checks
checks: licenses lint copyright

.PHONY: rabbitmq-server
rabbitmq-server:
	@docker run --detach --rm --name goamqp-rabbitmq \
		--publish 5672:5672 --publish 15672:15672 \
		--env RABBITMQ_DEFAULT_USER=user \
		--env RABBITMQ_DEFAULT_PASS=password \
		--env RABBITMQ_DEFAULT_VHOST=test \
		--pull always rabbitmq:4-management

.PHONY: stop-rabbitmq-server
stop-rabbitmq-server:
	@docker stop $$(docker inspect --format='{{.Id}}' goamqp-rabbitmq)

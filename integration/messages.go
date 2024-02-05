//go:build integration
// +build integration

package _integration

type Incoming struct {
	Query string
}

type Test struct {
	Test string
}

type IncomingResponse struct {
	Value string
}

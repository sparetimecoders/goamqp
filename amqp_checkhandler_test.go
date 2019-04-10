package go_amqp

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type missingProcess struct{}

func (missingProcess) Type() interface{} {
	return nil
}

type Message struct{}

func TestCheckHandlerMissingMethod(t *testing.T) {
	_, err := checkHandler(&missingProcess{})
	assert.EqualError(t, err, "missing method Process on handler, go_amqp.missingProcess")
}
type incorrectProcessArgumentCount struct{}

func (incorrectProcessArgumentCount) Process() {}

func (incorrectProcessArgumentCount) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectNumberOfArguments(t *testing.T) {
	_, err := checkHandler(&incorrectProcessArgumentCount{})
	assert.EqualError(t, err, "incorrect number of arguments, expected 1 but was 0")
}

type incorrectProcessInput struct{}

func (incorrectProcessInput) Process(msg string) {}

func (incorrectProcessInput) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectArguments(t *testing.T) {
	_, err := checkHandler(&incorrectProcessInput{})
	assert.EqualError(t, err, "incorrect in arguments. Expected Process(go_amqp.Message), actual Process(string)")
}

type missingReturnValue struct{}

func (missingReturnValue) Process(msg Message) {}

func (missingReturnValue) Type() interface{} { return Message{} }

func TestCheckHandlerMissingReturnValue(t *testing.T) {
	_, err := checkHandler(&missingReturnValue{})
	assert.EqualError(t, err, "incorrect number of return values. Expected 1, actual 0")
}

type incorrectProcessReturn struct{}

func (incorrectProcessReturn) Process(msg Message) string {return ""}

func (incorrectProcessReturn) Type() interface{} { return Message{} }

func TestCheckHandlerIncorrectReturn(t *testing.T) {
	_, err := checkHandler(&incorrectProcessReturn{})
	assert.EqualError(t, err, "incorrect return type for Process(go_amqp.Message). Expected bool, actual string")
}

type correctIncomingMessageHandler struct{}

func (correctIncomingMessageHandler) Process(msg Message) bool {return true}

func (correctIncomingMessageHandler) Type() interface{} { return Message{} }

func TestCheckHandlerCorrectIncomingMessageHandler(t *testing.T) {
	_, err := checkHandler(&correctIncomingMessageHandler{})
	assert.NoError(t, err)
}

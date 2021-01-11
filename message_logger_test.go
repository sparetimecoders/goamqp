package goamqp

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestStdoutLogger(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer func() { _ = os.Remove(file.Name()) }()
	logger := StdOutMessageLogger()
	stdout := os.Stdout
	os.Stdout = file
	logger([]byte(`{"key":"value"}`), reflect.TypeOf(Connection{}), "key", false)
	os.Stdout = stdout
	file.Close()
	all, err := ioutil.ReadFile(file.Name())
	require.NoError(t, err)
	require.Equal(t, "Received [goamqp.Connection] from routingkey: 'key' with content:\n{\n\t\"key\": \"value\"\n}\n", string(all))
}

package container_control

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleInOut(t *testing.T) {
	connector := DockerConnectorImpl{}
	result, err := connector.Run("quay.io/joconnel/io-test-script")

	assert.NoError(t, err)

	// Write the input to the script
	var input_to_container = []byte("abc\n")
	result.Write(input_to_container)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, result)

	assert.NoError(t, err)

	// The output should contain the expected output and the input given.
	assert.Contains(t, buf.String(), "This is what input was received: \"abc\"")
}

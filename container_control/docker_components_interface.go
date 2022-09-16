package container_control

import (
	"io"
)

type DockerConnector interface {
	// Pull, create, and run container in a local Docker instance.
	Run(image string) (*DockerContainer, error)
}

type DockerContainer interface {
	// Provide option to read from the standard output.
	io.Reader
	// Provide option to write to the standard input.
	io.Writer
	// Send a SIGTERM to the container, and then SIGKILL if it doesn't terminate.
	io.Closer
}

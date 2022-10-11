package kubernetes

import (
	"context"
	"io"

	"k8s.io/api/core/v1"
)

type connectorContainer struct {
	pod          *v1.Pod
	connector    connector
	stdinWriter  *io.PipeWriter
	stdoutReader *io.PipeReader
}

func (c connectorContainer) Read(p []byte) (n int, err error) {
	return c.stdoutReader.Read(p)
}

func (c connectorContainer) Write(p []byte) (n int, err error) {
	return c.stdinWriter.Write(p)
}

func (c connectorContainer) Close() error {
	if err := c.connector.removePod(context.Background(), c.pod, false); err != nil {
		return err
	}
	return nil
}

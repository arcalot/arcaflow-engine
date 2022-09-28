package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type connectorContainer struct {
	id               string
	hijackedResponse *types.HijackedResponse
	cli              *client.Client

	multiplexedReader
}

func (c connectorContainer) Write(p []byte) (n int, err error) {
	return c.hijackedResponse.Conn.Write(p)
}

func (c connectorContainer) Close() error {
	c.hijackedResponse.Close()
	_ = c.hijackedResponse.CloseWrite()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.cli.ContainerRemove(ctx, c.id, types.ContainerRemoveOptions{
		Force: true,
	}); err != nil {
		if !client.IsErrNotFound(err) {
			return fmt.Errorf("failed to remove container %s (%w)", c.id, err)
		}
	}
	return nil
}

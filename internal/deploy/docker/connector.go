package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
)

type connector struct {
	cli *client.Client
}

func (c connector) Deploy(ctx context.Context, image string) (deployer.Plugin, error) {
	if err := c.pullImage(ctx, image); err != nil {
		return nil, err
	}

	log.Printf("Creating container from image %s...", image)

	cnt, err := c.createContainer(image)
	if err != nil {
		return nil, err
	}

	if err := c.attachContainer(ctx, cnt); err != nil {
		return nil, err
	}

	//nolint:godox
	// TODO: Make this sleep workaround no longer needed.
	// It's required to not crash podman
	time.Sleep(500 * time.Millisecond)

	if err := c.startContainer(ctx, cnt); err != nil {
		return nil, err
	}

	log.Printf("Container started.")

	return cnt, nil
}

func (c connector) startContainer(ctx context.Context, cnt *connectorContainer) error {
	log.Printf("Starting container %s...", cnt.id)
	if err := c.cli.ContainerStart(ctx, cnt.id, types.ContainerStartOptions{}); err != nil {
		if err := cnt.Close(); err != nil {
			log.Printf("failed to remove previously-created container %s (%v)", cnt.id, err)
		}
		return fmt.Errorf("failed to start container %s (%w)", cnt.id, err)
	}
	return nil
}

func (c connector) attachContainer(ctx context.Context, cnt *connectorContainer) error {
	log.Printf("Attaching to container %s...", cnt.id)
	hijackedResponse, err := c.cli.ContainerAttach(
		ctx,
		cnt.id,
		types.ContainerAttachOptions{
			Stream: true,
			Stdin:  true,
			Stdout: true,
			Stderr: true,
			Logs:   true,
		},
	)
	if err != nil {
		if err := cnt.Close(); err != nil {
			log.Printf("failed to remove previously-created container %s (%v)", cnt.id, err)
		}
		return fmt.Errorf("failed to attach to container %s (%w)", cnt.id, err)
	}
	cnt.hijackedResponse = &hijackedResponse
	return nil
}

func (c connector) createContainer(image string) (*connectorContainer, error) {
	cont, err := c.cli.ContainerCreate(context.TODO(),
		&container.Config{
			Image:       image,
			Tty:         false,
			AttachStdin: true,
			StdinOnce:   true,
			OpenStdin:   true,
		}, nil, nil, nil, "",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create container from image %s (%w)", image, err)
	}

	cnt := &connectorContainer{
		id:  cont.ID,
		cli: c.cli,
	}
	return cnt, nil
}

func (c connector) pullImage(ctx context.Context, image string) error {
	log.Printf("Pulling image image %s...", image)
	pullReader, err := c.cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image %s (%w)", image, err)
	}
	if _, err := io.Copy(os.Stdout, pullReader); err != nil {
		return fmt.Errorf("failed to pull image %s (%w)", image, err)
	}
	return nil
}

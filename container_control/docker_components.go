package container_control

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type DockerConnectorImpl struct {
}

type DockerContainerImpl struct {
	hijackedResponse *types.HijackedResponse
}

func (c DockerConnectorImpl) Run(image string) (DockerContainer, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	println("Pulling image")

	pull_reader, err := cli.ImagePull(ctx, image, types.ImagePullOptions{})

	io.Copy(os.Stdout, pull_reader)

	if err != nil {
		return nil, err
	}

	println("Creating container")

	cont, err := cli.ContainerCreate(context.TODO(),
		&container.Config{
			Image:       image,
			Tty:         false,
			AttachStdin: true,
			StdinOnce:   true,
			OpenStdin:   true,
		}, nil, nil, nil, "",
	)

	if err != nil {
		return nil, err
	}

	println("Attaching container")

	hijackedResponse, err := cli.ContainerAttach(
		context.Background(),
		cont.ID,
		types.ContainerAttachOptions{
			Stream: true,
			Stdin:  true,
			Stdout: true,
			Stderr: true,
			Logs:   true,
		},
	)
	if err != nil {
		return nil, err
	}

	// TODO: Make this sleep workaround no longer needed.
	// It's required to not crash podman
	time.Sleep(500 * time.Millisecond)

	println("Starting container")

	err = cli.ContainerStart(context.TODO(), cont.ID, types.ContainerStartOptions{})

	if err != nil {
		return nil, err
	}

	println("Done")

	result := DockerContainerImpl{
		hijackedResponse: &hijackedResponse,
	}

	return result, nil
}

func (c DockerContainerImpl) Read(p []byte) (n int, err error) {
	return c.hijackedResponse.Reader.Read(p)
}

func (c DockerContainerImpl) Write(p []byte) (n int, err error) {
	return c.hijackedResponse.Conn.Write(p)
}

func (c DockerContainerImpl) Close() error {
	return nil
}

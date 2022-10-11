package docker

import (
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// Config is the configuration structure of the Docker connector.
type Config struct {
	Connection Connection `json:"connection"`
	Deployment Deployment `json:"deployment"`
	Timeouts   Timeouts   `json:"timeouts"`
}

// Validate checks the configuration structure for conformance with the schema.
func (c *Config) Validate() error {
	return Schema.ValidateType(c)
}

// Connection describes how to connect to the Docker daemon.
type Connection struct {
	Host   string `json:"host"`
	CACert string `json:"cacert"`
	Cert   string `json:"cert"`
	Key    string `json:"key"`
}

// ImagePullPolicy drives when an image should be pulled.
type ImagePullPolicy string

const (
	// ImagePullPolicyAlways means that the container image will be pulled for every workflow run.
	ImagePullPolicyAlways ImagePullPolicy = "Always"
	// ImagePullPolicyIfNotPresent means the image will be pulled if the image is not present locally, an empty tag, or
	// the "latest" tag was specified.
	ImagePullPolicyIfNotPresent ImagePullPolicy = "IfNotPresent"
	// ImagePullPolicyNever means that the image will never be pulled, and if the image is not available locally the
	// execution will fail.
	ImagePullPolicyNever ImagePullPolicy = "Never"
)

// Deployment contains the information about deploying the plugin.
type Deployment struct {
	ContainerConfig *container.Config         `json:"container"`
	HostConfig      *container.HostConfig     `json:"host"`
	NetworkConfig   *network.NetworkingConfig `json:"network"`
	Platform        *specs.Platform           `json:"platform"`

	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy"`
}

// Timeouts drive the timeouts for various interactions in relation to Docker.
type Timeouts struct {
	HTTP time.Duration `json:"http"`
}

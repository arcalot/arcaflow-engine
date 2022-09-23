package docker

import (
	"fmt"

	"github.com/docker/docker/client"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// NewFactory creates a new factory for the Docker deployer.
func NewFactory() deployer.ConnectorFactory[*Config] {
	return &factory{}
}

type factory struct {
}

func (f factory) ConfigurationSchema() schema.ScopeType[*Config] {
	return schema.NewScopeType[*Config](
		map[string]schema.ObjectType[any]{
			"Config": schema.NewObjectType[*Config](
				"Config",
				map[string]schema.PropertyType{},
			).Any(),
		},
		"Config",
	)
}

func (f factory) Create(config *Config) (deployer.Connector, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker container (%w)", err)
	}

	return &connector{
		cli,
	}, nil
}

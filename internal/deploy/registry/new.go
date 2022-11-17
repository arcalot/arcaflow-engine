package registry

import (
	"fmt"

	"go.flow.arcalot.io/deployer"
)

// New creates a new registry with the given factories.
func New(factory ...deployer.AnyConnectorFactory) Registry {
	factories := make(map[string]deployer.AnyConnectorFactory, len(factory))

	for _, f := range factory {
		if v, ok := factories[f.ID()]; ok {
			panic(fmt.Errorf("duplicate deployer factory ID: %s (first: %T, second: %T)", f.ID(), v, f))
		}
		factories[f.ID()] = f
	}

	return &registry{
		factories,
	}
}

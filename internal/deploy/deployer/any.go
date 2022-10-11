package deployer

import (
	"go.arcalot.io/log"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Any wraps a factory and creates an anonymous factory from it.
func Any[T any](factory ConnectorFactory[T]) AnyConnectorFactory {
	return &anyDeployerFactory[T]{
		factory: factory,
	}
}

type anyDeployerFactory[T any] struct {
	factory ConnectorFactory[T]
}

func (a anyDeployerFactory[T]) ID() string {
	return a.factory.ID()
}

func (a anyDeployerFactory[T]) ConfigurationSchema() schema.Object {
	return a.factory.ConfigurationSchema()
}

func (a anyDeployerFactory[T]) Create(config any, logger log.Logger) (Connector, error) {
	return a.factory.Create(config.(T), logger)
}

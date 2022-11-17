package registry

import (
	"fmt"
	"reflect"

	"go.arcalot.io/log"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Registry describes the functions a deployer registry must implement.
type Registry interface {
	// List lists the registered deployers with their scopes.
	List() map[string]schema.Object
	// Schema returns a composite schema for the registry.
	Schema() schema.OneOf[string]
	// Create creates a connector with the given configuration type. The registry must identify the correct deployer
	// based on the type passed.
	Create(config any, logger log.Logger) (deployer.Connector, error)
}

type registry struct {
	deployerFactories map[string]deployer.AnyConnectorFactory
}

func (r registry) List() map[string]schema.Object {
	result := make(map[string]schema.Object, len(r.deployerFactories))
	for id, factory := range r.deployerFactories {
		result[id] = factory.ConfigurationSchema()
	}
	return result
}

func (r registry) Schema() schema.OneOf[string] {
	schemas := make(map[string]schema.Object, len(r.deployerFactories))
	for id, factory := range r.deployerFactories {
		schemas[id] = factory.ConfigurationSchema()
	}
	return schema.NewOneOfStringSchema[any](
		schemas,
		"type",
	)
}

func (r registry) Create(config any, logger log.Logger) (deployer.Connector, error) {
	if config == nil {
		return nil, fmt.Errorf("the deployer configuration cannot be nil")
	}
	reflectedConfig := reflect.ValueOf(config)
	for _, factory := range r.deployerFactories {
		if factory.ConfigurationSchema().ReflectedType() == reflectedConfig.Type() {
			return factory.Create(config, logger)
		}
	}
	return nil, fmt.Errorf("could not identify correct deployer factory for %T", config)
}

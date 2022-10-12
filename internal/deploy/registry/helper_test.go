package registry_test

import (
	"context"
	"fmt"

	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

type testConfig struct {
}

type testNewFactory struct {
}

func (t testNewFactory) ID() string {
	return "test"
}

func (t testNewFactory) ConfigurationSchema() schema.Object {
	return schema.NewTypedScopeSchema[testConfig](
		schema.NewStructMappedObjectSchema[testConfig](
			"test",
			map[string]*schema.PropertySchema{},
		),
	)
}

func (t testNewFactory) Create(_ any) (deployer.Connector, error) {
	return &testConnector{}, nil
}

type testConnector struct {
}

func (t testConnector) Deploy(_ context.Context, _ string) (deployer.Plugin, error) {
	return nil, fmt.Errorf("not implemented")
}

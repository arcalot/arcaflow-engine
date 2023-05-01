package plugin_test

import (
	"go.arcalot.io/assert"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	deployer_registry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	testdeployer "go.flow.arcalot.io/testdeployer"
	"testing"
)

func TestProvider(t *testing.T) {
	logConfig := log.Config{
		Level:       log.LevelError,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	workflow_deployer_cfg := map[string]any{
		"type": "test-impl",
	}
	d_registry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))
	p, err := plugin.New(
		logger,
		d_registry,
		workflow_deployer_cfg,
	)
	if err != nil {
		panic(err)
	}
	assert.Equals(t, p.Kind(), "plugin")

	step_schema := map[string]any{
		"plugin": "simulation",
	}
	byte_schema := map[string][]byte{}
	// throws a nil pointer dereference
	runnable_step, err := p.LoadSchema(step_schema, byte_schema)
	assert.NoError(t, err)
	runnable_step.RunSchema()
}

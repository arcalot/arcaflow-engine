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
	deployer_cfg := testdeployer.Config{
		DeployTime: 2,
	}
	d_registry := deployer_registry.New(deployer.Any(testdeployer.NewFactory()))
	p, err := plugin.New(
		logger,
		d_registry,
		deployer_cfg,
	)
	if err != nil {
		panic(err)
	}
	//assert.NotNil(p)
	assert.Equals(t, p.Kind(), "plugin")
}

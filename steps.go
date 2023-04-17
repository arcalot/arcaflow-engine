package engine

import (
	"fmt"

	"go.arcalot.io/log/v2"
	deployerRegistry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/foreach"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	stepRegistry "go.flow.arcalot.io/engine/internal/step/registry"
)

// NewDefaultStepRegistry creates a registry with the default step types applied.
func NewDefaultStepRegistry(
	logger log.Logger,
	deployerRegistry deployerRegistry.Registry,
	config *config.Config,
) (step.Registry, error) {
	pluginProvider, err := plugin.New(logger, deployerRegistry, config.LocalDeployer)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin step provider (%w)", err)
	}

	loopProvider, err := foreach.New(logger, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create loop step provider (%w)", err)
	}

	stepR, err := stepRegistry.New(
		pluginProvider,
		loopProvider,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create step registry (%w)", err)
	}
	return stepR, nil
}

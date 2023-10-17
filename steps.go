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
	"go.flow.arcalot.io/engine/workflow"
)

// NewDefaultStepRegistry creates a registry with the default step types applied.
func NewDefaultStepRegistry(
	logger log.Logger,
	deployerRegistry deployerRegistry.Registry,
	config *config.Config,
) (step.Registry, error) {
	pluginProvider, err := plugin.New(logger, deployerRegistry, config.LocalDeployers)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin step provider (%w)", err)
	}

	workflowFactory := &workflowFactory{
		config: config,
	}

	loopProvider, err := foreach.New(logger, workflowFactory.createYAMLParser, workflowFactory.createWorkflow)
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
	workflowFactory.stepRegistry = stepR
	return stepR, nil
}

type workflowFactory struct {
	stepRegistry step.Registry
	config       *config.Config
}

func (f *workflowFactory) createYAMLParser() (workflow.YAMLConverter, error) {
	stepR := f.stepRegistry
	if stepR == nil {
		return nil, fmt.Errorf("YAML converter not available yet, please call the factory function after the engine has initialized")
	}
	return workflow.NewYAMLConverter(stepR), nil
}

func (f *workflowFactory) createWorkflow(logger log.Logger) (workflow.Executor, error) {
	stepR := f.stepRegistry
	if stepR == nil {
		return nil, fmt.Errorf("YAML converter not available yet, please call the factory function after the engine has initialized")
	}
	return workflow.NewExecutor(logger, f.config, stepR)
}

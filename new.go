package engine

import (
	"go.arcalot.io/log"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
)

// New creates a new workflow engine with the provided configuration. The passed deployerRegistry is responsible for
// providing deployment plugins.
func New(
	config *config.Config,
	deployerRegistry registry.Registry,
) (WorkflowEngine, error) {
	logger := log.New(config.Log)
	return &workflowEngine{
		logger:           logger,
		config:           config,
		deployerRegistry: deployerRegistry,
	}, nil
}

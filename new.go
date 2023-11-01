package engine

import (
	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
)

// New creates a new workflow engine with the provided configuration. The passed deployerRegistry is responsible for
// providing deployment plugins.
func New(
	config *config.Config,
) (WorkflowEngine, error) {
	logger := log.New(config.Log)

	stepRegistry, err := NewDefaultStepRegistry(logger,
		DefaultDeployerRegistry, config)
	if err != nil {
		return nil, err
	}
	return &workflowEngine{
		logger:       logger,
		config:       config,
		stepRegistry: stepRegistry,
	}, nil
}

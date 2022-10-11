package engine

import (
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/engine/internal/deploy/docker"
	"go.flow.arcalot.io/engine/internal/deploy/kubernetes"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
)

// DefaultDeployerRegistry contains the deployers.
var DefaultDeployerRegistry = registry.New(
	deployer.Any(docker.NewFactory()),
	deployer.Any(kubernetes.NewFactory()),
)

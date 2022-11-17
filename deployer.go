package engine

import (
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/dockerdeployer"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
	"go.flow.arcalot.io/kubernetesdeployer"
)

// DefaultDeployerRegistry contains the deployers.
var DefaultDeployerRegistry = registry.New(
	deployer.Any(docker.NewFactory()),
	deployer.Any(kubernetes.NewFactory()),
)

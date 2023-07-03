package engine

import (
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/deployer/registry"
	docker "go.flow.arcalot.io/dockerdeployer"
	kubernetes "go.flow.arcalot.io/kubernetesdeployer"
	podman "go.flow.arcalot.io/podmandeployer"
	python "go.flow.arcalot.io/pythondeployer"
)

// DefaultDeployerRegistry contains the deployers.
var DefaultDeployerRegistry = registry.New(
	deployer.Any(docker.NewFactory()),
	deployer.Any(kubernetes.NewFactory()),
	deployer.Any(podman.NewFactory()),
	deployer.Any(python.NewFactory()),
)

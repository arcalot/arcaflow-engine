package engine

import (
	"fmt"
	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/deployer/registry"
	docker "go.flow.arcalot.io/dockerdeployer"
	"go.flow.arcalot.io/engine/config"
	kubernetes "go.flow.arcalot.io/kubernetesdeployer"
	podman "go.flow.arcalot.io/podmandeployer"
	python "go.flow.arcalot.io/pythondeployer"
	testimpl "go.flow.arcalot.io/testdeployer"
)

// New creates a new workflow engine with the provided configuration. The passed deployerRegistry is responsible for
// providing deployment plugins.
func New(
	config *config.Config,
) (WorkflowEngine, error) {
	logger := log.New(config.Log)
	// TODO: Create new registry function
	// need to build/create new deployer registry based on
	// deployer_ids in config.LocalDeployers
	// use registry.Create and its reflection as inspo
	//for key, value := range config.LocalDeployers {
	//
	//}
	reg, err := BuildRegistry(config.LocalDeployers)
	if err != nil {
		return nil, err
	}
	stepRegistry, err := NewDefaultStepRegistry(
		logger,
		reg,
		config,
	)
	if err != nil {
		return nil, err
	}
	return &workflowEngine{
		logger:       logger,
		config:       config,
		stepRegistry: stepRegistry,
	}, nil
}

//var DefaultDeployerRegistry = registry.New(
//	deployer.Any(docker.NewFactory()),
//	deployer.Any(kubernetes.NewFactory()),
//	deployer.Any(podman.NewFactory()),
//	deployer.Any(python.NewFactory()),
//)

func BuildRegistry(config map[string]any) (registry.Registry, error) {
	if config == nil {
		return nil, fmt.Errorf("the deployer configuration cannot be nil")
	}

	factories := make([]deployer.AnyConnectorFactory, 0)

	//workshops := make([]deployer.AnyConnectorFactory, 0)

	for deploymentType, value := range config {
		schemas := DefaultDeployerRegistry.DeployConfigSchema(deployer.DeploymentType(deploymentType))
		//v2 := make(map[string]any)
		//for k, v := range value.(map[any]any) {
		//	v2[k.(string)] = v
		//}

		//var f deployer.AnyConnectorFactory
		//switch v2[schemas.DiscriminatorFieldName()] {
		//case "docker":
		//	f = deployer.Any(docker.NewFactory())
		//case "podman":
		//	f = deployer.Any(podman.NewFactory())
		//case "kubernetes":
		//	f = deployer.Any(kubernetes.NewFactory())
		//case "python":
		//	f = deployer.Any(python.NewFactory())
		//}
		//
		//factories = append(factories, f)

		unserializedConfig, err := schemas.Unserialize(value)
		if err != nil {
			return nil, err
		}

		//fmt.Printf("%v\n", unserializedConfig.(*docker.Config))
		//var f2 deployer.AnyConnectorFactory
		switch unserializedConfig.(type) {
		case *docker.Config:
			//f2 = deployer.Any(docker.NewFactory())
			factories = append(factories, deployer.Any(docker.NewFactory()))
		case *podman.Config:
			//f2 = deployer.Any(podman.NewFactory())
			factories = append(factories, deployer.Any(podman.NewFactory()))
		case *kubernetes.Config:
			//f2 = deployer.Any(kubernetes.NewFactory())
			factories = append(factories, deployer.Any(kubernetes.NewFactory()))
		case *python.Config:
			//f2 = deployer.Any(python.NewFactory())
			factories = append(factories, deployer.Any(python.NewFactory()))
		default: // deployer stub config
			factories = append(factories, deployer.Any(testimpl.NewFactory()))
		}

		//workshops = append(workshops, f2)
		//factories = append(factories, f2)
	}

	return registry.New(factories...), nil
}

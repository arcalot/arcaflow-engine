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

type cfg struct{}

func BuildRegistry(config map[string]any) (registry.Registry, error) {
	if config == nil {
		return nil, fmt.Errorf("the deployer configuration cannot be nil")
	}

	//for _,v := range DefaultDeployerRegistry {
	//fmt.Printf("%v\n" v)
	//}

	factories := make([]deployer.AnyConnectorFactory, 0)
	for deploymentType, value := range config {
		v2 := make(map[string]any)
		for k, v := range value.(map[any]any) {
			v2[k.(string)] = v
		}
		schemas := DefaultDeployerRegistry.DeployConfigSchema(deployer.DeploymentType(deploymentType))
		//ss = append(ss, schemas)
		var f deployer.AnyConnectorFactory
		switch v2[schemas.DiscriminatorFieldName()] {
		case "docker":
			f = deployer.Any(docker.NewFactory())
		case "podman":
			f = deployer.Any(podman.NewFactory())
		case "kubernetes":
			f = deployer.Any(kubernetes.NewFactory())
		case "python":
			f = deployer.Any(python.NewFactory())
		}

		factories = append(factories, f)
		//unserializedConfig, err := schemas.Unserialize(value)

		//if err != nil {
		//	return nil, err
		//}
		//reflectedConfig := reflect.ValueOf(unserializedConfig)
		//fmt.Printf("%v\n", reflectedConfig)

		//reglist := DefaultDeployerRegistry.List()
		//for _, factory := range reglist {
		//	if factory.ReflectedType() == reflectedConfig.Type() {
		//dc := deployer.DeploymentConfig(unserializedConfig)
		//unserializedConfig.(deployer.DeploymentConfig).NewFactory()
		//t := reflect.TypeOf(unserializedConfig)
		//v := reflect.ValueOf(unserializedConfig)
		//fmt.Printf("%v\n", t)
		//fmt.Printf("%v\n", v)
		//d0 := reflect.ValueOf(unserializedConfig).Type()
		//dc := deployer.DeploymentConfig[any]{}
		////dc := deployer.DeploymentConfig[reflect.TypeFor(unserializedConfig)]{}
		//dc := deployer.DeploymentConfig[cfg](unserializedConfig)
		//factories = append(factories, unserializedConfig)
		//fmt.Printf("%v\n", unserializedConfig)
		//factories = append(factories,
		//	deployer.Any(
		//		dc.NewFactory(),
		//	),
		//)
		//}
		//}
		//}
	}

	return registry.New(factories...), nil
}

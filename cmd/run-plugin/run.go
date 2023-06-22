// Package main provides a simple script to run a plugin as a standalone.
package main

import (
	"context"
	"flag"
	"fmt"
	"go.flow.arcalot.io/deployer"
	podman "go.flow.arcalot.io/podmandeployer"
	"os"
	"os/signal"

	log "go.arcalot.io/log/v2"
	docker "go.flow.arcalot.io/dockerdeployer"
	"go.flow.arcalot.io/pluginsdk/atp"
	testdeployer "go.flow.arcalot.io/testdeployer"
	"gopkg.in/yaml.v3"
)

func main() {
	var image string
	var file string
	var stepID string
	var d deployer.AnyConnectorFactory
	var defaultConfig any
	var deployerID = "docker"

	flag.StringVar(&image, "image", image, "Docker image to run")
	flag.StringVar(&file, "file", file, "Input file")
	flag.StringVar(&stepID, "step", stepID, "Step name")
	flag.StringVar(&deployerID, "deployer", stepID, "The name of the deployer")
	flag.Parse()

	switch deployerID {
	case "docker":
		dockerFactory := docker.NewFactory()
		d = deployer.Any(dockerFactory)

		configSchema := dockerFactory.ConfigurationSchema()
		var err error
		defaultConfig, err = configSchema.UnserializeType(map[string]any{})
		if err != nil {
			panic(err)
		}
	case "podman":
		podmanFactory := podman.NewFactory()
		d = deployer.Any(podmanFactory)
		configSchema := podmanFactory.ConfigurationSchema()
		var err error
		defaultConfig, err = configSchema.UnserializeType(map[string]any{})
		if err != nil {
			panic(err)
		}
	case "testimpl":
		podmanFactory := testdeployer.NewFactory()
		d = deployer.Any(podmanFactory)
		configSchema := podmanFactory.ConfigurationSchema()
		var err error
		defaultConfig, err = configSchema.UnserializeType(map[string]any{})
		if err != nil {
			panic(err)
		}
	default:
		panic("No deployer selected. Options: docker, podman, testimpl. Select with -deployer")
	}

	connector, err := d.Create(defaultConfig, log.New(log.Config{
		Level:       log.LevelDebug,
		Destination: log.DestinationStdout,
	}))
	if err != nil {
		panic(err)
	}
	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctrlC:
			fmt.Println("Received CTRL-C. Cancelling the context to cancel the step...")
			cancel()
		case <-ctx.Done():
			// Done here.
		}
	}()

	fmt.Println("Deploying")
	plugin, err := connector.Deploy(ctx, image)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := plugin.Close(); err != nil {
			panic(err)
		}
	}()

	atpClient := atp.NewClient(plugin)
	fmt.Println("Getting schema")
	pluginSchema, err := atpClient.ReadSchema()
	if err != nil {
		panic(err)
	}
	steps := pluginSchema.Steps()
	step, ok := steps[stepID]
	if !ok {
		panic(fmt.Errorf("no such step: %s", stepID))
	}
	inputContents, err := os.ReadFile(file) //nolint:gosec
	if err != nil {
		panic(err)
	}
	input := map[string]any{}
	if err := yaml.Unmarshal(inputContents, &input); err != nil {
		panic(err)
	}
	if _, err := step.Input().Unserialize(input); err != nil {
		panic(err)
	}
	fmt.Printf("Running step %s\n", stepID)
	outputID, outputData, err := atpClient.Execute(stepID, input)
	output := map[string]any{
		"outputID":   outputID,
		"outputData": outputData,
		"err":        err,
	}
	result, err := yaml.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", result)
}

// Package main provides a simple script to run a plugin as a standalone.
package main

import (
	"context"
	"flag"
	"fmt"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/pluginsdk/plugin"
	"go.flow.arcalot.io/pluginsdk/schema"
	podman "go.flow.arcalot.io/podmandeployer"
	pythondeployer "go.flow.arcalot.io/pythondeployer"
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
	var pythonPath string
	var d deployer.AnyConnectorFactory
	var defaultConfig any
	var deployerID = "docker"
	var runID = "run"
	var workingDir string

	flag.StringVar(&image, "image", image, "Docker image to run")
	flag.StringVar(&file, "file", file, "Input file")
	flag.StringVar(&stepID, "step", stepID, "Step name")
	flag.StringVar(&deployerID, "deployer", stepID, "The name of the deployer")
	flag.StringVar(&pythonPath, "pythonpath", "", "Path to the Python environment")
	flag.StringVar(&workingDir, "workingdir", "~/", "Path to store cloned repositories")
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
	case "python":
		pythonFactory := pythondeployer.NewFactory()
		d = deployer.Any(pythonFactory)
		configSchema := pythonFactory.ConfigurationSchema()
		var err error
		configInput := map[string]any{}
		configInput["pythonPath"] = pythonPath
		configInput["workdir"] = workingDir
		defaultConfig, err = configSchema.UnserializeType(configInput)
		if err != nil {
			panic(err)
		}
	default:
		panic("No deployer or invalid deployer selected. Options: docker, podman, testimpl, python. Select with -deployer")
	}
	logger := log.New(log.Config{
		Level:       log.LevelDebug,
		Destination: log.DestinationStdout,
	})
	connector, err := d.Create(defaultConfig, logger)
	if err != nil {
		panic(err)
	}
	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	// Set up the signal channel to send cancel signal on ctrl-c
	toStepSignals := make(chan schema.Input, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctrlC:
			logger.Infof("Received CTRL-C. Sending cancel signal...")
			toStepSignals <- schema.Input{
				RunID:     runID,
				ID:        plugin.CancellationSignalSchema.ID(),
				InputData: make(map[string]any),
			}
			logger.Infof("Signal request sent to ATP client.")
			cancel()
		case <-ctx.Done():
			// Done here.
		}
	}()

	logger.Infof("Deploying")
	plugin, err := connector.Deploy(ctx, image)
	if err != nil {
		logger.Errorf("Error while deploying: %s", err)
		panic(err)
	}
	defer func() {
		if err := plugin.Close(); err != nil {
			panic(err)
		}
	}()

	atpClient := atp.NewClientWithLogger(plugin, logger)
	logger.Infof("Getting schema")
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
	result := atpClient.Execute(
		schema.Input{RunID: runID, ID: stepID, InputData: input},
		toStepSignals,
		nil,
	)
	if err := atpClient.Close(); err != nil {
		fmt.Printf("Error closing ATP client: %s", err)
	}
	output := map[string]any{
		"outputID":   result.OutputID,
		"outputData": result.OutputData,
		"err":        result.Error,
	}
	resultStr, err := yaml.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", resultStr)
}

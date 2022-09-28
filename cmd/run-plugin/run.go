package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.flow.arcalot.io/engine/internal/deploy/docker"
	"go.flow.arcalot.io/pluginsdk/atp"
	"gopkg.in/yaml.v3"
)

//nolint:funlen
func main() {
	var image string
	var file string
	var stepID string

	flag.StringVar(&image, "image", image, "Docker image to run")
	flag.StringVar(&file, "file", file, "Input file")
	flag.StringVar(&stepID, "step", stepID, "Step name")
	flag.Parse()

	d := docker.NewFactory()
	configSchema := d.ConfigurationSchema()
	defaultConfig, err := configSchema.UnserializeType(map[string]any{})
	if err != nil {
		panic(err)
	}
	connector, err := d.Create(defaultConfig)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	pluginSchema, err := atpClient.ReadSchema()
	if err != nil {
		panic(err)
	}
	steps := pluginSchema.Steps()
	step, ok := steps[stepID]
	if !ok {
		panic(fmt.Errorf("No such step: %s", stepID))
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
	outputID, outputData, debugLogs := atpClient.Execute(ctx, stepID, input)
	output := map[string]any{
		"outputID":   outputID,
		"outputData": outputData,
		"debugLogs":  debugLogs,
	}
	result, err := yaml.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", result)
}

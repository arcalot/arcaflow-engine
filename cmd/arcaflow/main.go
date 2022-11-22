// Package main provides the main entrypoint for Arcaflow.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.arcalot.io/log/v2"

	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
	"gopkg.in/yaml.v3"
)

func main() {
	tempLogger := log.New(log.Config{
		Level:       log.LevelInfo,
		Destination: log.DestinationStdout,
		Stdout:      os.Stderr,
	})

	configFile := ""
	input := ""
	dir := "."
	workflow := "workflow.yaml"

	flag.StringVar(
		&configFile,
		"config",
		configFile,
		"The Arcaflow configuration file to load, if any.",
	)
	flag.StringVar(
		&input,
		"input",
		input,
		"The workflow input file to load. May be outside the workflow directory. If no input file is passed, "+
			"the workflow is assumed to take no input.",
	)
	flag.StringVar(
		&dir,
		"context",
		dir,
		"The workflow directory to run from. Defaults to the current directory.",
	)
	flag.StringVar(
		&workflow,
		"workflow",
		workflow,
		"The workflow file in the current directory to load. Defaults to workflow.yaml.",
	)
	flag.Usage = func() {
		_, _ = os.Stderr.Write([]byte(`Usage: arcaflow [OPTIONS]

The Arcaflow engine will read the current directory and use it as a context
for executing the workflow.

Options:

  -config FILENAME    The Arcaflow configuration file to load, if any.

  -input FILENAME     The workflow input file to load. May be outside the
                      workflow directory. If no input file is passed,
                      the workflow is assumed to take no input.

  -context DIRECTORY  The workflow directory to run from. Defaults to the
                      current directory.

  -workflow FILENAME  The workflow file in the current directory to load.
                      Defaults to workflow.yaml.
`))
	}
	flag.Parse()

	var err error
	var configData any = map[any]any{}
	if configFile != "" {
		configData, err = loadYamlFile(configFile)
		if err != nil {
			tempLogger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
			flag.Usage()
			os.Exit(1)
		}
	}
	cfg, err := config.Load(configData)
	if err != nil {
		tempLogger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(1)
	}
	cfg.Log.Stdout = os.Stderr

	logger := log.New(cfg.Log)

	dirContext, err := loadContext(dir)
	if err != nil {
		logger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(1)
	}

	flow, err := engine.New(cfg)
	if err != nil {
		logger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(1)
	}

	var inputData []byte
	if input != "" {
		inputData, err = os.ReadFile(input)
		if err != nil {
			logger.Errorf("Failed to read input file %s (%v)", input, err)
			flag.Usage()
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outputData, err := flow.RunWorkflow(ctx, inputData, dirContext, workflow)
	if err != nil {
		logger.Errorf("Workflow execution failed (%v)", err)
		os.Exit(1) //nolint:gocritic
	}
	data, err := yaml.Marshal(outputData)
	if err != nil {
		logger.Errorf("Failed to marshal output (%v)", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(data)
}

func loadContext(dir string) (map[string][]byte, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain absolute path of context directory %s (%w)", dir, err)
	}
	result := map[string][]byte{}
	err = filepath.Walk(absDir,
		func(path string, i os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !i.IsDir() {
				fileData, err := os.ReadFile(path) //nolint:gosec
				if err != nil {
					return fmt.Errorf("failed to read file from context directory: %s (%w)", path, err)
				}
				path = strings.TrimPrefix(path, absDir)
				path = strings.TrimPrefix(path, string([]byte{os.PathSeparator}))
				result[path] = fileData
			}
			return nil
		})
	return result, err
}

func loadYamlFile(configFile string) (any, error) {
	fileContents, err := os.ReadFile(configFile) //nolint:gosec
	if err != nil {
		return nil, err
	}
	var data any
	if err := yaml.Unmarshal(fileContents, &data); err != nil {
		return nil, err
	}
	return data, nil
}

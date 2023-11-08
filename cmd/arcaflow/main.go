// Package main provides the main entrypoint for Arcaflow.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"go.arcalot.io/log/v2"

	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
	"gopkg.in/yaml.v3"
)

// These variables are filled using ldflags during the build process with Goreleaser.
// See https://goreleaser.com/cookbooks/using-main.version/
var (
	version = "development"
	commit  = "unknown"
	date    = "unknown"
)

// ExitCodeOK signals that the program terminated normally.
const ExitCodeOK = 0

// ExitCodeInvalidData signals that the program encountered an invalid workflow or input data.
const ExitCodeInvalidData = 1

// ExitCodeWorkflowErrorOutput indicates that the workflow executed successfully, but terminated with an output
// marked as error.
const ExitCodeWorkflowErrorOutput = 2

// ExitCodeWorkflowFailed indicates that the workflow execution failed.
const ExitCodeWorkflowFailed = 3

func main() {
	tempLogger := log.New(log.Config{
		Level:       log.LevelInfo,
		Destination: log.DestinationStdout,
		Stdout:      os.Stderr,
	})

	configFile := ""
	input := ""
	dir := "."
	workflowFile := "workflow.yaml"
	printVersion := false

	flag.BoolVar(&printVersion, "version", printVersion, "Print Arcaflow Engine version and exit.")
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
		&workflowFile,
		"workflow",
		workflowFile,
		"The workflow file in the current directory to load. Defaults to workflow.yaml.",
	)
	flag.Usage = func() {
		_, _ = os.Stderr.Write([]byte(`Usage: arcaflow [OPTIONS]

The Arcaflow engine will read the current directory and use it as a context
for executing the workflow.

Options:

  -version            Print the Arcaflow Engine version and exit.

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

	if printVersion {
		fmt.Printf(
			"Arcaflow Engine\n"+
				"===============\n"+
				"Version: %s\n"+
				"Commit: %s\n"+
				"Date: %s\n"+
				"Apache 2.0 license\n"+
				"Copyright (c) Arcalot Contributors",
			version, commit, date,
		)
		return
	}

	var err error
	var configData any = map[any]any{}
	if configFile != "" {
		configData, err = loadYamlFile(configFile)
		if err != nil {
			tempLogger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
			flag.Usage()
			os.Exit(ExitCodeInvalidData)
		}
	}
	cfg, err := config.Load(configData)
	if err != nil {
		tempLogger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(ExitCodeInvalidData)
	}
	cfg.Log.Stdout = os.Stderr

	logger := log.New(cfg.Log).WithLabel("source", "main")

	dirContext, err := loadContext(dir)
	if err != nil {
		logger.Errorf("Failed to load configuration file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(ExitCodeInvalidData)
	}

	flow, err := engine.New(cfg)
	if err != nil {
		logger.Errorf("Failed to initialize engine with config file %s (%v)", configFile, err)
		flag.Usage()
		os.Exit(ExitCodeInvalidData)
	}

	var inputData []byte
	if input != "" {
		inputData, err = os.ReadFile(input)
		if err != nil {
			logger.Errorf("Failed to read input file %s (%v)", input, err)
			flag.Usage()
			os.Exit(ExitCodeInvalidData)
		}
	}

	os.Exit(runWorkflow(flow, dirContext, workflowFile, logger, inputData))
}

func runWorkflow(flow engine.WorkflowEngine, dirContext map[string][]byte, workflowFile string, logger log.Logger, inputData []byte) int {
	ctx, cancel := context.WithCancel(context.Background())
	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	go handleOSInterrupt(ctrlC, cancel, logger)
	defer func() {
		close(ctrlC) // Ensure that the goroutine exits
		cancel()
	}()

	workflow, err := flow.Parse(dirContext, workflowFile)
	if err != nil {
		logger.Errorf("Invalid workflow (%v)", err)
		return ExitCodeInvalidData
	}

	outputID, outputData, outputError, err := workflow.Run(ctx, inputData)
	if err != nil {
		logger.Errorf("Workflow execution failed (%v)", err)
		return ExitCodeWorkflowFailed
	}
	data, err := yaml.Marshal(
		map[string]any{
			"output_id":   outputID,
			"output_data": outputData,
		},
	)
	if err != nil {
		logger.Errorf("Failed to marshal output (%v)", err)
		return ExitCodeInvalidData
	}
	_, _ = os.Stdout.Write(data)
	if outputError {
		return ExitCodeWorkflowErrorOutput
	}
	return ExitCodeOK
}

func handleOSInterrupt(ctrlC chan os.Signal, cancel context.CancelFunc, logger log.Logger) {
	_, ok := <-ctrlC
	if !ok {
		return
	}
	logger.Infof("Requesting graceful shutdown.")
	cancel()

	_, ok = <-ctrlC
	if !ok {
		return
	}
	logger.Warningf("Hit CTRL-C again to forcefully exit workflow without cleanup. You may need to manually delete pods or containers.")

	_, ok = <-ctrlC
	if !ok {
		return
	}
	logger.Warningf("Force exiting. You may need to manually delete pods or containers.")
	os.Exit(1)
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

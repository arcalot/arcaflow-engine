// Package main provides the main entrypoint for Arcaflow.
package main

import (
	"context"
	"flag"
	"fmt"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/loadfile"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
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

// RequiredFileKeyWorkflow is the key for the workflow file in hash map of files required for execution.
const RequiredFileKeyWorkflow = "workflow"

// RequiredFileKeyConfig is the key for the config file in hash map of files required for execution.
const RequiredFileKeyConfig = "config"

// RequiredFileKeyInput is the key for the input file in hash map of files required for execution.
const RequiredFileKeyInput = "input"

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

	requiredFiles := map[string]string{
		RequiredFileKeyConfig:   configFile,
		RequiredFileKeyInput:    input,
		RequiredFileKeyWorkflow: workflowFile,
	}

	fileCtx, err := loadfile.NewFileCacheUsingContext(dir, requiredFiles)
	if err != nil {
		flag.Usage()
		tempLogger.Errorf("context path resolution failed %s (%v)", dir, err)
		os.Exit(ExitCodeInvalidData)
	}

	var configData any = map[any]any{}
	if configFile != "" {
		configData, err = loadYamlFile(*fileCtx.AbsPathByKey(RequiredFileKeyConfig))
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

	// now we are ready to instantiate our main logger
	cfg.Log.Stdout = os.Stderr
	logger := log.New(cfg.Log).WithLabel("source", "main")

	err = fileCtx.LoadContext()
	if err != nil {
		logger.Errorf("Failed to load required files into context (%v)", err)
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

	os.Exit(runWorkflow(flow, fileCtx, RequiredFileKeyWorkflow, logger, inputData))
}

func runWorkflow(flow engine.WorkflowEngine, fileCtx loadfile.FileCache, workflowFile string, logger log.Logger, inputData []byte) int {
	ctx, cancel := context.WithCancel(context.Background())
	ctrlC := make(chan os.Signal, 4) // We expect up to three ctrl-C inputs. Plus one extra to buffer in case.
	signal.Notify(ctrlC, os.Interrupt)

	go handleOSInterrupt(ctrlC, cancel, logger)
	defer func() {
		close(ctrlC) // Ensure that the goroutine exits
		cancel()
	}()

	workflow, err := flow.Parse(fileCtx, workflowFile)
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

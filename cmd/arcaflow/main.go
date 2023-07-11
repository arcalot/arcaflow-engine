// Package main provides the main entrypoint for Arcaflow.
package main

import (
	"context"
	"flag"
	"fmt"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
	"gopkg.in/yaml.v3"
	"reflect"

	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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

// ExitCodeUserInterrupt indicates that the engine was interrupted and terminated by the system.
const ExitCodeUserInterrupt = 4

// Signal table
var signals = [...]string{
	0: "ok",
	1: "invalid input data",
	2: "workflow output error",
	3: "workflow failed",
	4: "user interrupt",
}

type ArcaflowExitSignal int

func (s ArcaflowExitSignal) Signal() {}

func (s ArcaflowExitSignal) String() string {
	if 0 <= s && int(s) < len(signals) {
		str := signals[s]
		if str != "" {
			return str
		}
	}
	return "signal " + strconv.Itoa(int(s))
}

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

	logger := log.New(cfg.Log)

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

	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT)
	//signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Infof("Starting workflow")
	var exitCode int
	var sig os.Signal
	go func() {
		select {
		case sig = <-sigs:
			// Got sigterm. So cancel context.
			//logger.Infof("Caught signal %s", sig)
			//exitCode = ExitCodeUserInterrupt
			exitCode = osSignalToInt(sig)
			cancel()
		case <-ctx.Done():
			// Done. No sigint.
			logger.Infof("Main context done")
			//exitCode = ExitCodeOK
		}
	}()

	//exitCode = <-runWorkflow(flow, dirContext, workflowFile, logger, inputData, ctx)
	//var exitCode syscall.Signal

	exitCode = osSignalToInt(<-runWorkflow(sigs, flow, dirContext, workflowFile, logger, inputData, ctx))

	//go runWorkflow(sigs, flow, dirContext, workflowFile, logger, inputData, ctx)
	//sig := <-sigs
	////handleSignal(sig, logger, cancel, ctx)
	//
	//switch sig {
	//case os.Interrupt:
	//	logger.Infof("Caught signal %s", sig)
	//	cancel()
	//	//ctx.Done()
	//case os.Kill:
	//	logger.Infof("Caught signal %s", sig)
	//	cancel()
	//	//ctx.Done()
	//}
	//exitCode := osSignalToInt(sig)

	logger.Infof("Got exit code %d", exitCode)
	os.Exit(exitCode)
}

func runWorkflow(sigs chan os.Signal, flow engine.WorkflowEngine, dirContext map[string][]byte, workflowFile string, logger log.Logger, inputData []byte, ctx context.Context) chan os.Signal {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan os.Signal, 1)
	defer close(out)

	workflow, err := flow.Parse(dirContext, workflowFile)
	if err != nil {
		logger.Errorf("Invalid workflow (%v)", err)
		out <- ArcaflowExitSignal(ExitCodeInvalidData)
		return out
	}

	outputID, outputData, outputError, err := workflow.Run(ctx, inputData)
	if err != nil {
		logger.Errorf("Workflow execution failed (%v)", err)
		out <- ArcaflowExitSignal(ExitCodeWorkflowFailed)
		return out
	}
	data, err := yaml.Marshal(
		map[string]any{
			"output_id":   outputID,
			"output_data": outputData,
		},
	)
	if err != nil {
		logger.Errorf("Failed to marshal output (%v)", err)
		out <- ArcaflowExitSignal(ExitCodeInvalidData)
		return out
	}
	_, _ = os.Stdout.Write(data)
	if outputError {
		out <- ArcaflowExitSignal(ExitCodeWorkflowErrorOutput)
		return out
	}

	out <- ArcaflowExitSignal(ExitCodeOK)
	return out
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

func handleSignal(sig os.Signal, logger log.Logger, cancel context.CancelFunc, ctx context.Context) {
	switch sig {
	case os.Interrupt:
		logger.Infof("Caught signal %s", sig)
		//cancel()
		ctx.Done()
	case os.Kill:
		logger.Infof("Caught signal %s", sig)
		//cancel()
		ctx.Done()
	}
}

var (
	OK                  ArcaflowExitSignal = ExitCodeOK
	InvalidData         ArcaflowExitSignal = ExitCodeInvalidData
	WorkflowErrorOutput ArcaflowExitSignal = ExitCodeWorkflowErrorOutput
	WorkflowFailed      ArcaflowExitSignal = ExitCodeWorkflowFailed
)

func osSignalToInt(s os.Signal) int {
	fmt.Printf("type of signal: %v\n", reflect.TypeOf(s))
	switch s {
	case os.Interrupt:
		return int(s.(syscall.Signal))
	case os.Kill:
		return int(s.(syscall.Signal))
	case OK:
		return int(s.(ArcaflowExitSignal))
	case InvalidData:
		return int(s.(ArcaflowExitSignal))
	case WorkflowErrorOutput:
		return int(s.(ArcaflowExitSignal))
	case WorkflowFailed:
		return int(s.(ArcaflowExitSignal))
	}
	return -1
}

// Package main provides the main entrypoint for Arcaflow.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/loadfile"
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
	getNamespaces := false

	const (
		versionUsage       = "Print Arcaflow Engine version and exit."
		configUsage        = "The path to the Arcaflow configuration file to load, if any."
		inputUsage         = "The path to the workflow input file to load, if any."
		contextUsage       = "The path to the workflow directory to run from."
		workflowUsage      = "The path to the workflow file to load."
		getNamespacesUsage = "Show the namespaces available to this workflow."
	)
	flag.BoolVar(&printVersion, "version", printVersion, versionUsage)
	flag.StringVar(&configFile, "config", configFile, configUsage)
	flag.StringVar(&input, "input", input, inputUsage)
	flag.StringVar(&dir, "context", dir, contextUsage)
	flag.StringVar(&workflowFile, "workflow", workflowFile, workflowUsage)
	flag.BoolVar(&getNamespaces, "get-namespaces", getNamespaces, getNamespacesUsage)

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = w.Write(
			[]byte(
				"Usage: " + os.Args[0] + " [OPTIONS]\n\n" +
					"Arcaflow will read file paths relative to the context directory.\n\n",
			),
		)
		flag.PrintDefaults()
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
				"Copyright (c) Arcalot Contributors\n",
			version, commit, date,
		)
		return
	}

	var err error

	requiredFiles := map[string]string{
		RequiredFileKeyWorkflow: workflowFile,
	}

	if len(configFile) != 0 {
		requiredFiles[RequiredFileKeyConfig] = configFile
	}

	if len(input) != 0 {
		requiredFiles[RequiredFileKeyInput] = input
	}

	fileCtx, err := loadfile.NewFileCacheUsingContext(dir, requiredFiles)
	if err != nil {
		tempLogger.Errorf("Context path resolution failed %s (%v)", dir, err)
		flag.Usage()
		os.Exit(ExitCodeInvalidData)
	}

	var configData any
	// If no config file is passed, we use an empty map to accept the schema defaults
	configData = make(map[string]any)
	if len(configFile) > 0 {
		configFilePath, err := fileCtx.AbsPathByKey(RequiredFileKeyConfig)
		if err != nil {
			tempLogger.Errorf("Unable to find configuration file %s (%v)", configFile, err)
			flag.Usage()
			os.Exit(ExitCodeInvalidData)
		}

		configData, err = loadYamlFile(configFilePath)
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
	if len(input) == 0 {
		inputData = []byte("{}")
	} else {
		inputFilePath, err := fileCtx.AbsPathByKey(RequiredFileKeyInput)
		if err != nil {
			tempLogger.Errorf("Unable to find input file %s (%v)", input, err)
			flag.Usage()
			os.Exit(ExitCodeInvalidData)
		}

		inputData, err = os.ReadFile(filepath.Clean(inputFilePath))
		if err != nil {
			logger.Errorf("Failed to read input file %s (%v)", input, err)
			flag.Usage()
			os.Exit(ExitCodeInvalidData)
		}
	}

	os.Exit(runWorkflow(flow, fileCtx, RequiredFileKeyWorkflow, logger, inputData, getNamespaces))
}

func runWorkflow(flow engine.WorkflowEngine, fileCtx loadfile.FileCache, workflowFile string, logger log.Logger, inputData []byte, getNamespaces bool) int {
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

	if getNamespaces {
		//_, _ = os.Stdout.Write([]byte(buildNamespaceResponse(workflow)))
		printNamespaceResponse(os.Stdout, workflow, logger)
	} else {
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

func printNamespaceResponse(output io.Writer, workflow engine.Workflow, logger log.Logger) error {
	tabwriterPadding := 3
	w := tabwriter.NewWriter(output, 6, 4, tabwriterPadding, ' ', tabwriter.FilterHTML)
	columns := []string{"namespace", "object"}

	// write header
	for _, col := range columns {
		_, _ = fmt.Fprint(w, strings.ToUpper(col), "\t")
	}
	_, _ = fmt.Fprintln(w)

	// write each row
	allNamespaces := workflow.Namespaces()
	for namespace, objects := range allNamespaces {
		_, _ = fmt.Fprint(w, namespace, "\t")
		for objectID := range objects {
			_, _ = fmt.Fprint(w, objectID, " ")
		}
		_, _ = fmt.Fprintln(w)
	}
	_, _ = fmt.Fprintln(w)

	_ = w.Flush()
	return nil
}

//func buildNamespaceResponse(workflow engine.Workflow) string {
//	namespaceTblStr := "Available objects and their namespaces:\n"
//	namespaceTblStr += "NAMESPACE\tOBJECT\n"
//	namespaceTblStr += workflow.Namespaces()
//	return namespaceTblStr
//}

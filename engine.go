// Package engine provides an embeddable engine variant.
package engine

import (
	"context"
	"fmt"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/workflow"

	"go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/yaml"
)

// WorkflowEngine is responsible for executing workflows and returning their result.
type WorkflowEngine interface {
	// RunWorkflow executes a workflow from the passed workflow files as parameters. One of the files must be designated
	// as a workflow file, which will be parsed from the YAML format. Additional files may be passed so that the
	// workflow may access them (e.g. a kubeconfig file). The workflow input is passed as a separate file.
	RunWorkflow(
		ctx context.Context,
		input []byte,
		files map[string][]byte,
		workflowFileName string,
	) (
		outputData any,
		err error,
	)
}

type workflowEngine struct {
	logger           log.Logger
	deployerRegistry registry.Registry
	stepRegistry     step.Registry
	config           *config.Config
}

func (w workflowEngine) RunWorkflow(
	ctx context.Context,
	// The input file is a YAML data structure already read from a file (or stdin)
	input []byte,
	// The files are a map of files and their contents read from the workflow directory. This allows
	// the workflow to reference other files within the workflow directory.
	files map[string][]byte,
	// The workflow file name specifies which file is the actual workflow within the workflow directory.
	workflowFileName string,
) (outputData any, err error) {
	if workflowFileName == "" {
		workflowFileName = "workflow.yaml"
	}
	workflowContents, ok := files[workflowFileName]
	if !ok {
		return nil, ErrNoWorkflowFile
	}

	yamlConverter := workflow.NewYAMLConverter(w.stepRegistry)
	wf, err := yamlConverter.FromYAML(workflowContents)
	if err != nil {
		return nil, err
	}

	executor, err := workflow.NewExecutor(w.logger, w.stepRegistry)
	if err != nil {
		return nil, err
	}

	preparedWorkflow, err := executor.Prepare(wf, files)
	if err != nil {
		return nil, err
	}

	decodedInput, err := yaml.New().Parse(input)
	if err != nil {
		return nil, fmt.Errorf("failed to YAML decode input (%w)", err)
	}

	return preparedWorkflow.Execute(ctx, decodedInput.Raw())
}

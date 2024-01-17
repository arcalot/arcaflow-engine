// Package engine provides an embeddable engine variant.
package engine

import (
	"context"
	"fmt"
	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/engine/loadfile"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/schema"
)

var supportedVersions = map[string]struct{}{
	"v0.2.0": {},
}

// WorkflowEngine is responsible for executing workflows and returning their result.
type WorkflowEngine interface {
	// RunWorkflow is a simplified shortcut to parse and immediately run a workflow.
	RunWorkflow(
		ctx context.Context,
		input []byte,
		workflowContext loadfile.FileCache,
		workflowFileName string,
	) (outputID string, outputData any, outputError bool, err error)

	// Parse ingests a workflow context as a map of files to their contents and a workflow file name and
	// parses the data into an executable workflow.
	Parse(
		workflowContext loadfile.FileCache,
		workflowFileName string,
	) (
		workflow Workflow,
		err error,
	)
}

// Workflow is a runnable, queryable workflow. You can execute it, or query it for schema information.
type Workflow interface {
	// Run executes the workflow with the passed, YAML-formatted input data.
	Run(
		ctx context.Context,
		input []byte,
	) (
		outputID string,
		outputData any,
		outputIsError bool,
		err error,
	)

	// InputSchema returns the requested input schema for the workflow.
	InputSchema() schema.Scope
	// Outputs returns the list of possible outputs and their schema for the workflow.
	Outputs() map[string]schema.StepOutput
}

type workflowEngine struct {
	logger       log.Logger
	stepRegistry step.Registry
	config       *config.Config
}

func (w workflowEngine) RunWorkflow(
	ctx context.Context,
	input []byte,
	workflowContext loadfile.FileCache,
	workflowFileName string,
) (outputID string, outputData any, outputError bool, err error) {
	wf, err := w.Parse(workflowContext, workflowFileName)
	if err != nil {
		return "", nil, true, err
	}
	return wf.Run(ctx, input)
}

func (w workflowEngine) Parse(
	files loadfile.FileCache,
	workflowFileName string,
) (Workflow, error) {
	if workflowFileName == "" {
		workflowFileName = "workflow.yaml"
	}
	workflowContents := files.ContentByKey(workflowFileName)
	if workflowContents == nil {
		return nil, ErrNoWorkflowFile
	}

	yamlConverter := workflow.NewYAMLConverter(w.stepRegistry)
	wf, err := yamlConverter.FromYAML(workflowContents)
	if err != nil {
		return nil, err
	}

	stepFilePaths := map[string]string{}
	for _, stepData := range wf.Steps {
		stepDataMap, ok1 := stepData.(map[any]any)
		if ok1 {
			kind, ok1 := stepDataMap["kind"]
			if ok1 {
				kindString := kind.(string)
				if kindString == "foreach" {
					subworkflowPath := stepDataMap["workflow"]
					subworkflowPathString := subworkflowPath.(string)
					stepFilePaths[subworkflowPathString] = subworkflowPathString
				}
			}
		}
	}

	stepFileCache, err := loadfile.NewFileCache(files.RootDir, stepFilePaths)
	if err != nil {
		return nil, err
	}
	err = stepFileCache.LoadContext()
	if err != nil {
		return nil, err
	}
	fc := loadfile.MergeFileCaches(files, *stepFileCache)

	v, err := SupportedVersion(wf.Version)
	if err != nil {
		return nil, err
	}
	wf.Version = v

	executor, err := workflow.NewExecutor(w.logger, w.config, w.stepRegistry)
	if err != nil {
		return nil, err
	}

	preparedWorkflow, err := executor.Prepare(wf, fc.Contents())
	if err != nil {
		return nil, err
	}

	return &engineWorkflow{
		workflow: preparedWorkflow,
	}, nil
}

// SupportedVersion confirms whether a given version string
// is in the set of supported workflow specifications. It
// returns true when the version is in the set, false otherwise.
// Earlier schema validation already applies version's
// regular expression.
func SupportedVersion(version string) (string, error) {
	_, ok := supportedVersions[version]
	if !ok {
		return version, fmt.Errorf("unsupported workflow schema version: %s", version)
	}
	return version, nil
}

type engineWorkflow struct {
	workflow workflow.ExecutableWorkflow
}

func (e engineWorkflow) Run(
	ctx context.Context,
	input []byte,
) (outputID string, outputData any, outputIsError bool, err error) {
	decodedInput, err := yaml.New().Parse(input)
	if err != nil {
		return "", nil, true, fmt.Errorf("failed to YAML decode input (%w)", err)
	}

	outputID, outputData, err = e.workflow.Execute(ctx, decodedInput.Raw())
	if err != nil {
		return "", nil, true, err
	}
	outputSchema, ok := e.workflow.OutputSchema()[outputID]
	if !ok {
		return "", nil, true, fmt.Errorf("bug: the output schema has no output named '%s'", outputID)
	}
	return outputID, outputData, outputSchema.Error(), nil
}

func (e engineWorkflow) InputSchema() schema.Scope {
	return e.workflow.Input()
}

func (e engineWorkflow) Outputs() map[string]schema.StepOutput {
	outputSchema := e.workflow.OutputSchema()
	outputs := make(map[string]schema.StepOutput, len(outputSchema))
	for outputID, output := range outputSchema {
		outputs[outputID] = output
	}
	return outputs
}

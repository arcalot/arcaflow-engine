package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"go.arcalot.io/dgraph"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/infer"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/pluginsdk/schema"

	"go.flow.arcalot.io/engine/config"
)

// NewExecutor creates a new executor instance for workflows.
func NewExecutor(
	logger log.Logger,
	config *config.Config,
	stepRegistry step.Registry,
) (Executor, error) {
	if logger == nil {
		return nil, fmt.Errorf("bug: no logger passed to NewExecutor")
	}
	if stepRegistry == nil {
		return nil, fmt.Errorf("bug: no step registry passed to NewExecutor")
	}
	return &executor{
		logger:       logger,
		stepRegistry: stepRegistry,
		config:       config,
	}, nil
}

// Executor is a tool to execute workflows.
type Executor interface {
	// Prepare processes the workflow for execution. The workflow parameter should contain the workflow description,
	// where each step input consists only of primitive types or expressions. The workflowContext variable should
	// contain all files from around the workflow, which will allow for evaluating expression functions against these
	// additional files.
	Prepare(
		workflow *Workflow,
		workflowContext map[string][]byte,
	) (ExecutableWorkflow, error)
}

// ExecutableWorkflow is a workflow that has been prepared by the executor and is ready to be run.
type ExecutableWorkflow interface {
	// Input returns the input schema of the workflow.
	Input() schema.Scope

	// DAG returns the directed acyclic graph for this workflow.
	DAG() dgraph.DirectedGraph[*DAGItem]

	// Execute runs a workflow until it finishes or until the context expires with the specified input. The input
	// must only contain primitives (float, int, bool, string, map, slice) and may not contain structs and other
	// elements. The output will consist of the output ID, the returned output data corresponding to the output IDs
	// schema, or if an error happened, the error.
	Execute(
		ctx context.Context,
		input any,
	) (outputID string, outputData any, err error)

	// OutputSchema returns the schema for the possible outputs of this workflow.
	OutputSchema() map[string]*schema.StepOutputSchema
}

type executor struct {
	logger       log.Logger
	config       *config.Config
	stepRegistry step.Registry
}

// Prepare goes through all workflow steps and constructs their schema and input data.
//
// This function goes through 5 stages:
//
// 1. We only know the step provider (e.g. "plugin") and provide its basic input data. In the case of plugin
// this will be the name of the plugin container image.
//
// 2. We have queried the provider and now know more about the provider. In case we now have the schema of the
// plugin and can evaluate the "step" property if set. This will be needed to evaluate the final schema of the
// plugin.
//
// 3. We take the final schema and construct the property schema for the various stages of the execution.
// For "plugin" these will be the "deploy" and "input" stages.
//
// 4. After the loop, we can now create the dependency graph between the steps and their stages. This will
// result in an orderly execution of the individual stages.
//
// 5. We can now construct the output data model of the workflow.
func (e *executor) Prepare(workflow *Workflow, workflowContext map[string][]byte) (ExecutableWorkflow, error) {
	dag := dgraph.New[*DAGItem]()
	if _, err := dag.AddNode("input", &DAGItem{
		Kind: "input",
	}); err != nil {
		return nil, fmt.Errorf("failed to add input node (%w)", err)
	}

	// First we unserialize the input schema
	typedInput, err := e.processInput(workflow)
	if err != nil {
		return nil, err
	}

	// Then we process the steps. This involves several sub-steps, make sure to check the function.
	runnableSteps, stepOutputProperties, stepLifecycles, stepRunData, err := e.processSteps(workflow, dag, workflowContext)
	if err != nil {
		return nil, err
	}

	// Now we can construct an internal data model for the output data model provided by the steps. This is the schema
	// the expressions evaluate against. You can use this to do static code analysis on the expressions.
	internalDataModel := e.buildInternalDataModel(typedInput, stepOutputProperties)

	// Stage 4: build the DAG dependencies.
	if err := e.connectStepDependencies(workflow, workflowContext, stepLifecycles, dag, internalDataModel); err != nil {
		return nil, err
	}

	// Stage 5: The output data model
	//goland:noinspection GoDeprecation
	if workflow.Output != nil {
		if len(workflow.Outputs) > 0 {
			return nil, fmt.Errorf("both 'output' and 'outputs' is provided, please provide one")
		}
		//goland:noinspection GoDeprecation
		workflow.Outputs = map[string]any{
			"success": workflow.Output,
		}
	}
	if len(workflow.Outputs) == 0 {
		return nil, fmt.Errorf("no output provided for workflow")
	}
	outputsSchema := map[string]*schema.StepOutputSchema{}
	for outputID, outputData := range workflow.Outputs {
		var outputSchema *schema.StepOutputSchema
		if workflow.OutputSchema != nil && workflow.OutputSchema[outputID] != nil {
			outputSchemaData, err := schema.DescribeStepOutput().Unserialize(workflow.OutputSchema[outputID])
			if err != nil {
				return nil, fmt.Errorf("unable to decode workflow output schema %s (%w)", outputID, err)
			}
			outputSchema = outputSchemaData.(*schema.StepOutputSchema)
		}
		outputSchema, err = infer.OutputSchema(outputData, outputID, outputSchema, internalDataModel, workflowContext)
		if err != nil {
			return nil, fmt.Errorf("cannot read/infer workflow output schema for output %s (%w)", outputID, err)
		}
		outputsSchema[outputID] = outputSchema
		output := &DAGItem{
			Kind:         DAGItemKindOutput,
			OutputID:     outputID,
			Data:         outputData,
			OutputSchema: outputSchema,
		}
		outputNode, err := dag.AddNode(output.String(), output)
		if err != nil {
			return nil, fmt.Errorf("failed to add workflow output node %s to DAG (%w)", outputID, err)
		}
		if err := e.prepareDependencies(workflowContext, outputData, outputNode, internalDataModel, dag); err != nil {
			return nil, fmt.Errorf("failed to build dependency tree for output (%w)", err)
		}
	}

	// We don't like cycles as we can't execute them properly. Maybe we can improve this later to actually output the
	// cycle to help the user?
	if dag.HasCycles() {
		return nil, fmt.Errorf("your workflow has a cycle")
	}

	return &executableWorkflow{
		logger:            e.logger,
		config:            e.config,
		dag:               dag,
		input:             typedInput,
		stepRunData:       stepRunData,
		workflowContext:   workflowContext,
		internalDataModel: internalDataModel,
		runnableSteps:     runnableSteps,
		lifecycles:        stepLifecycles,
		outputSchema:      outputsSchema,
	}, nil
}

func (e *executor) processInput(workflow *Workflow) (schema.Scope, error) {
	scope, err := schema.DescribeScope().Unserialize(workflow.Input)
	if err != nil {
		return nil, &ErrInvalidWorkflow{fmt.Errorf("invalid workflow input section (%w)", err)}
	}
	typedInput, ok := scope.(schema.Scope)
	if !ok {
		return nil, fmt.Errorf("bug: unserialized input is not a scope")
	}
	typedInput.ApplyScope(typedInput)
	return typedInput, nil
}

func (e *executor) processSteps(
	workflow *Workflow,
	dag dgraph.DirectedGraph[*DAGItem],
	workflowContext map[string][]byte,
) (
	runnableSteps map[string]step.RunnableStep,
	stepOutputProperties map[string]*schema.PropertySchema,
	stepLifecycles map[string]step.Lifecycle[step.LifecycleStageWithSchema],
	stepRunData map[string]map[string]any,
	err error,
) {
	runnableSteps = make(map[string]step.RunnableStep, len(workflow.Steps))
	stepOutputProperties = make(map[string]*schema.PropertySchema, len(workflow.Steps))
	stepLifecycles = make(map[string]step.Lifecycle[step.LifecycleStageWithSchema], len(workflow.Steps))
	stepRunData = make(map[string]map[string]any, len(workflow.Steps))
	for stepID, stepData := range workflow.Steps {
		stepDataMap, ok := stepData.(map[any]any)
		if !ok {
			return nil, nil, nil, nil, &ErrInvalidWorkflow{fmt.Errorf("step %s has an invalid type: %T, expected: map", stepID, stepData)}
		}
		kind, ok := stepDataMap["kind"]
		if !ok {
			// For backwards compatibility
			kind = "plugin"
		}
		kindString, ok := kind.(string)
		if !ok {
			return nil, nil, nil, nil, fmt.Errorf("step %s is invalid ('kind' field should be a string, %T found)", stepID, kind)
		}
		stepKind, err := e.stepRegistry.GetByKind(kindString)
		if err != nil {
			return nil, nil, nil, nil, &ErrInvalidWorkflow{fmt.Errorf("step %s is invalid (%w)", stepID, err)}
		}

		// Stage 1: unserialize the data with only the provider properties known.
		runnableStep, err := e.loadSchema(stepKind, stepID, stepDataMap, workflowContext)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		runnableSteps[stepID] = runnableStep

		// Stage 2: unserialize the data with the provider and runnable properties being known.
		runData, err := e.getRunData(stepKind, runnableStep, stepID, stepDataMap)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		stepRunData[stepID] = runData

		typedLifecycle, err := runnableStep.Lifecycle(runData)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"failed to get lifecycle for step %s (%w)",
				stepID,
				err,
			)
		}
		stepLifecycles[stepID] = typedLifecycle

		// Stage 3: construct a schema for the outputs of each stage that the expressions can query.
		outputProperties, err := e.buildOutputProperties(typedLifecycle, stepID, runnableStep, dag)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		stepOutputProperties[stepID] = schema.NewPropertySchema(
			schema.NewObjectSchema(
				stepID,
				outputProperties,
			),
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	return runnableSteps, stepOutputProperties, stepLifecycles, stepRunData, nil
}

// connectStepDependencies connects the steps based on their expressions.
func (e *executor) connectStepDependencies(
	workflow *Workflow,
	workflowContext map[string][]byte,
	stepLifecycles map[string]step.Lifecycle[step.LifecycleStageWithSchema],
	dag dgraph.DirectedGraph[*DAGItem],
	internalDataModel *schema.ScopeSchema,
) error {
	for stepID, stepData := range workflow.Steps {
		indexableStepData := stepData.(map[any]any)
		lifecycle := stepLifecycles[stepID]
		for _, stage := range lifecycle.Stages {
			currentStageNode, err := dag.GetNodeByID(GetStageNodeID(stepID, stage.ID))
			if err != nil {
				return fmt.Errorf("bug: node for current stage not found (%w)", err)
			}
			for _, nextStage := range stage.NextStages {
				if err := currentStageNode.Connect(GetStageNodeID(stepID, nextStage)); err != nil {
					return fmt.Errorf("bug: cannot connect nodes (%w)", err)
				}
			}
			stageData := make(map[any]any, len(stage.InputFields))
			for inputField := range stage.InputFields {
				data := indexableStepData[inputField]
				if data != nil {
					stageData[inputField] = data
				}
				if err := e.prepareDependencies(workflowContext, data, currentStageNode, internalDataModel, dag); err != nil {
					return fmt.Errorf("failed to build dependency tree for %s (%w)", currentStageNode.ID(), err)
				}
			}
			currentStageNode.Item().Data = stageData
			if len(stage.InputSchema) > 0 {
				currentStageNode.Item().DataSchema = schema.NewObjectSchema(
					"input",
					stage.InputSchema,
				)
			}
		}
	}
	return nil
}

// buildInternalDataModel builds an internal data model that the expressions can query.
func (e *executor) buildInternalDataModel(input schema.Scope, stepOutputProperties map[string]*schema.PropertySchema) *schema.ScopeSchema {
	internalDataModel := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"workflow",
			map[string]*schema.PropertySchema{
				"input": schema.NewPropertySchema(
					input,
					schema.NewDisplayValue(
						schema.PointerTo("Input"),
						schema.PointerTo("Input definitions for this workflow. These are used to render the form for starting the workflow."),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"steps": schema.NewPropertySchema(
					schema.NewObjectSchema(
						"steps",
						stepOutputProperties,
					),
					nil,
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
			},
		),
	)
	return internalDataModel
}

func (e *executor) buildOutputProperties(
	typedLifecycle step.Lifecycle[step.LifecycleStageWithSchema],
	stepID string,
	runnableStep step.RunnableStep,
	dag dgraph.DirectedGraph[*DAGItem],
) (map[string]*schema.PropertySchema, error) {
	outputProperties := map[string]*schema.PropertySchema{}
	for _, stage := range typedLifecycle.Stages {
		stepDAGItem := &DAGItem{
			Kind:     DAGItemKindStepStage,
			StepID:   stepID,
			StageID:  stage.ID,
			OutputID: "",
			Provider: runnableStep,
		}
		stepNode, err := dag.AddNode(stepDAGItem.String(), stepDAGItem)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to add stage %s in step %s to DAG (%w)",
				stage.ID,
				stepID,
				err,
			)
		}
		stageOutputProperties := make(map[string]*schema.PropertySchema, len(stage.Outputs))
		for outputID, outputSchema := range stage.Outputs {
			stageDAGItem := &DAGItem{
				Kind:     DAGItemKindStepStageOutput,
				StepID:   stepID,
				StageID:  stage.ID,
				OutputID: outputID,
				Provider: runnableStep,
			}
			stageNode, err := dag.AddNode(stageDAGItem.String(), stageDAGItem)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to add output %s for stage %s in step %s to DAG (%w)",
					outputID,
					stage.ID,
					stepID,
					err,
				)
			}
			if err := stepNode.Connect(stageNode.ID()); err != nil {
				return nil, fmt.Errorf(
					"failed to connect stage %s to its output %s in step %s (%w)",
					stage.ID, outputID, stepID, err,
				)
			}
			stageOutputProperties[outputID] = schema.NewPropertySchema(
				outputSchema.Schema(),
				outputSchema.Display(),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
		}

		stageOutputs := schema.NewObjectSchema(
			GetStageNodeID(stepID, stage.ID),
			stageOutputProperties,
		)

		outputProperties[stage.ID] = schema.NewPropertySchema(
			stageOutputs,
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	return outputProperties, nil
}

func (e *executor) getRunData(stepKind step.Provider, runnableStep step.RunnableStep, stepID string, stepDataMap map[any]any) (map[string]any, error) {
	properties := stepKind.ProviderSchema()
	properties["kind"] = schema.NewPropertySchema(
		schema.NewStringEnumSchema(
			map[string]*schema.DisplayValue{
				stepKind.Kind(): nil,
			},
		),
		nil,
		false,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	runProperties := runnableStep.RunSchema()
	for property, propertyValue := range runProperties {
		properties[property] = propertyValue
	}
	for _, lf := range stepKind.Lifecycle().Stages {
		for inputField := range lf.InputFields {
			properties[inputField] = schema.NewPropertySchema(
				newAnySchemaWithExpressions(),
				nil,
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
		}
	}
	fullRunSchema := schema.NewObjectSchema(
		stepID,
		properties,
	)
	runData, err := fullRunSchema.Unserialize(stepDataMap)
	if err != nil {
		return nil, &ErrInvalidWorkflow{fmt.Errorf("invalid step configuration for %s (%w)", stepID, err)}
	}
	runSchema := runnableStep.RunSchema()
	result := make(map[string]any, len(runSchema))
	for runField := range runSchema {
		result[runField] = runData.(map[string]any)[runField]
	}
	return result, nil
}

func (e *executor) loadSchema(stepKind step.Provider, stepID string, stepDataMap map[any]any, workflowContext map[string][]byte) (step.RunnableStep, error) {
	properties := stepKind.ProviderSchema()
	properties["kind"] = schema.NewPropertySchema(
		schema.NewStringEnumSchema(
			map[string]*schema.DisplayValue{
				stepKind.Kind(): nil,
			},
		),
		nil,
		false,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	// Add the additional properties to avoid unserialization errors.
	for runProperty := range stepKind.RunProperties() {
		properties[runProperty] = schema.NewPropertySchema(
			schema.NewAnySchema(),
			nil,
			false,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	for _, lf := range stepKind.Lifecycle().Stages {
		for inputField := range lf.InputFields {
			properties[inputField] = schema.NewPropertySchema(
				newAnySchemaWithExpressions(),
				nil,
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
		}
	}
	stepProviderSchema := schema.NewObjectSchema(
		stepID,
		properties,
	)
	unserializedStepData, err := stepProviderSchema.Unserialize(stepDataMap)
	if err != nil {
		return nil, &ErrInvalidWorkflow{fmt.Errorf("invalid step configuration for %s (%w)", stepID, err)}
	}
	providerSchema := stepKind.ProviderSchema()
	providerData := make(map[string]any, len(providerSchema))
	for field := range providerSchema {
		providerData[field] = unserializedStepData.(map[string]any)[field]
	}
	runnableStep, err := stepKind.LoadSchema(providerData, workflowContext)
	if err != nil {
		return nil, &ErrInvalidWorkflow{fmt.Errorf("failed to load schema for step %s (%w)", stepID, err)}
	}
	return runnableStep, nil
}

func (e *executor) prepareDependencies( //nolint:gocognit,gocyclo
	workflowContext map[string][]byte,
	stepData any,
	currentNode dgraph.Node[*DAGItem],
	outputSchema *schema.ScopeSchema,
	dag dgraph.DirectedGraph[*DAGItem],
) error {
	if stepData == nil {
		return nil
	}
	t := reflect.TypeOf(stepData)
	switch t.Kind() {
	case reflect.Bool:
		fallthrough
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.String:
		return nil
	case reflect.Ptr:
		fallthrough
	case reflect.Struct:
		switch s := stepData.(type) {
		case expressions.Expression:
			dependencies, err := s.Dependencies(outputSchema, workflowContext)
			if err != nil {
				return fmt.Errorf(
					"failed to evaluate dependencies of the expression %s (%w)",
					s.String(),
					err,
				)
			}
			for _, dependency := range dependencies {
				dependencyKind := dependency[1]
				switch dependencyKind {
				case "input":
					inputNode, err := dag.GetNodeByID("input")
					if err != nil {
						return fmt.Errorf("failed to find input node (%w)", err)
					}
					if err := inputNode.Connect(currentNode.ID()); err != nil {
						decodedErr := &dgraph.ErrConnectionAlreadyExists{}
						if !errors.As(err, &decodedErr) {
							return fmt.Errorf("failed to connect input to %s (%w)", currentNode.ID(), err)
						}
					}
				case "steps":
					var prevNodeID string
					switch dependencyNodes := len(dependency); {
					case dependencyNodes == 4: // Example: $.steps.example.outputs
						prevNodeID = dependency[1:4].String()
					case dependencyNodes >= 5: // Example: $.steps.example.outputs.success (or longer)
						prevNodeID = dependency[1:5].String()
					default:
						return fmt.Errorf("invalid dependency %s", dependency.String())
					}
					prevNode, err := dag.GetNodeByID(prevNodeID)
					if err != nil {
						return fmt.Errorf("failed to find depending node %s (%w)", prevNodeID, err)
					}
					if err := prevNode.Connect(currentNode.ID()); err != nil {
						decodedErr := &dgraph.ErrConnectionAlreadyExists{}
						if !errors.As(err, &decodedErr) {
							return fmt.Errorf("failed to connect DAG node (%w)", err)
						}
					}
				default:
					return fmt.Errorf("bug: invalid dependency kind: %s", dependencyKind)
				}
			}
			return nil
		default:
			return &ErrInvalidWorkflow{fmt.Errorf("unsupported struct/pointer type in workflow input: %T", stepData)}
		}
	case reflect.Slice:
		v := reflect.ValueOf(stepData)
		for i := 0; i < v.Len(); i++ {
			value := v.Index(i).Interface()
			if err := e.prepareDependencies(workflowContext, value, currentNode, outputSchema, dag); err != nil {
				return wrapDependencyError(currentNode.ID(), fmt.Sprintf("%d", i), err)
			}
		}
		return nil
	case reflect.Map:
		v := reflect.ValueOf(stepData)
		for _, reflectedKey := range v.MapKeys() {
			key := reflectedKey.Interface()
			value := v.MapIndex(reflectedKey).Interface()
			if err := e.prepareDependencies(workflowContext, value, currentNode, outputSchema, dag); err != nil {
				return wrapDependencyError(currentNode.ID(), fmt.Sprintf("%v", key), err)
			}
		}
		return nil
	default:
		return &ErrInvalidWorkflow{fmt.Errorf("unsupported primitive type: %T", stepData)}
	}
}

// DependencyError describes an error while preparing dependencies.
type DependencyError struct {
	ID      string   `json:"id"`
	Path    []string `json:"path"`
	Message string   `json:"message"`
	Cause   error    `json:"cause"`
}

// Error returns the error message.
func (d DependencyError) Error() string {
	return fmt.Sprintf("dependency error in step %s: '%s' (%s)", d.ID, strings.Join(d.Path, "' -> '"), d.Message)
}

// Unwrap returns the original error that caused this error.
func (d DependencyError) Unwrap() error {
	return d.Cause
}

func wrapDependencyError(id string, pathItem string, err error) error {
	var depError *DependencyError
	if errors.As(err, &depError) {
		return &DependencyError{
			ID:      id,
			Path:    append(depError.Path, pathItem),
			Message: err.Error(),
			Cause:   err,
		}
	}
	return &DependencyError{
		ID:      id,
		Path:    []string{pathItem},
		Message: err.Error(),
		Cause:   err,
	}
}

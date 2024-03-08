package workflow

import (
	"fmt"
	"regexp"
	"strings"

	"go.arcalot.io/dgraph"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Workflow is the primary data structure describing workflows.
type Workflow struct {
	// Version determines which set of the arcaflow workflow external interface will be used in the workflow.
	Version string `json:"version"`
	// Input describe the input schema for a workflow. These values can be referenced from expressions. The structure
	// must be a scope described in primitive types. This is done so later on a forward reference to a step input can
	// be used.
	Input any `json:"input"`
	// Steps contains the possible steps in this workflow. The data set must contain a valid step structure where the
	// inputs to stages may consist only of primitive types and expressions.
	Steps map[string]any `json:"steps"`
	// Outputs lets you define one or more outputs. The outputs should be keyed by their output ID (e.g. "success") and
	// the value should be the data you wish to output. The data may contain expressions to construct the output.
	Outputs map[string]any `json:"outputs"`
	// OutputSchema is an optional override for the automatically inferred output schema from the Outputs data and
	// expressions. The keys must be the output IDs from Outputs and the values must be a StepOutputSchema object as
	// per the Arcaflow schema.
	OutputSchema map[string]*schema.StepOutputSchema `json:"outputSchema"`
	// Output is the legacy way to define a single output. It conflicts the "outputs" field and if filled, will create a
	// "success" output.
	//
	// Deprecated: use Outputs instead.
	Output any `json:"output"`
}

// getSchema returns the entire workflow schema.
func getSchema() *schema.TypedScopeSchema[*Workflow] {
	return schema.NewTypedScopeSchema[*Workflow](
		schema.NewStructMappedObjectSchema[*Workflow](
			"Workflow",
			map[string]*schema.PropertySchema{
				"version": schema.NewPropertySchema(
					schema.NewStringSchema(
						schema.IntPointer(1),
						schema.IntPointer(255),
						regexp.MustCompile(`^v\d+\.\d+\.\d+$`)),
					schema.NewDisplayValue(
						schema.PointerTo("Version"),
						schema.PointerTo("Arcaflow Workflow specification version to be used."),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"input": schema.NewPropertySchema(
					schema.NewAnySchema(),
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
					schema.NewMapSchema(
						schema.NewStringSchema(
							schema.IntPointer(1),
							schema.IntPointer(255),
							regexp.MustCompile("^[$@a-zA-Z0-9-_]+$"),
						),
						newAnySchemaWithExpressions(),
						schema.IntPointer(1),
						nil,
					),
					schema.NewDisplayValue(
						schema.PointerTo("Steps"),
						schema.PointerTo("Workflow steps to execute in this workflow."),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"output": schema.NewPropertySchema(
					newAnySchemaWithExpressions(),
					schema.NewDisplayValue(
						schema.PointerTo("Output"),
						schema.PointerTo("Create a single output data structure for this workflow using expressions. This option is deprecated, use 'outputs' instead to create multiple possible outputs."),
						nil,
					),
					false,
					nil,
					[]string{"outputs"},
					[]string{"outputs", "outputSchema"},
					nil,
					nil,
				),
				"outputs": schema.NewPropertySchema(
					schema.NewMapSchema(
						schema.NewStringSchema(
							schema.PointerTo[int64](1),
							nil,
							regexp.MustCompile("^[$@a-zA-Z0-9-_]+$"),
						),
						newAnySchemaWithExpressions(),
						schema.PointerTo[int64](1),
						nil,
					),
					schema.NewDisplayValue(
						schema.PointerTo("Outputs"),
						schema.PointerTo("Output data, possibly containing expressions."),
						nil,
					),
					false,
					[]string{"outputSchema"},
					[]string{"output"},
					[]string{"output"},
					nil,
					nil,
				),
				"outputSchema": schema.NewPropertySchema(
					schema.NewMapSchema(
						schema.NewStringSchema(
							schema.PointerTo[int64](1),
							nil,
							regexp.MustCompile("^[$@a-zA-Z0-9-_]+$"),
						),
						schema.DescribeStepOutput(),
						schema.PointerTo[int64](1),
						nil,
					),
					schema.NewDisplayValue(
						schema.PointerTo("Output schema"),
						schema.PointerTo("Explicitly override the schema of the outputs. The schema for outputs that are not explicitly specified here will be inferred."),
						nil,
					),
					false,
					nil,
					nil,
					[]string{"output"},
					nil,
					nil,
				),
			},
		),
	)
}

// DAGItemKind is the type of DAG node items.
type DAGItemKind string

const (
	// DAGItemKindInput indicates a DAG node for the workflow input.
	DAGItemKindInput DAGItemKind = "input"
	// DAGItemKindStepStage indicates a DAG node for a stage.
	DAGItemKindStepStage DAGItemKind = "stepStage"
	// DAGItemKindStepStageOutput indicates a DAG node for an output of a stage.
	DAGItemKindStepStageOutput DAGItemKind = "stepStageOutput"
	// DAGItemKindOutput indicates a DAG node for the workflow output.
	DAGItemKindOutput DAGItemKind = "output"
)

// DAGItem is the internal structure of the DAG.
type DAGItem struct {
	// Kind discriminates between the input and step nodes.
	Kind DAGItemKind
	// StepID is only filled for step types.
	StepID string
	// StageID is the stage of the step provider this item refers to.
	StageID string
	// OutputID is the ID of the output of the step stage.
	OutputID string
	// OutputSchema contains the output-specific schema for this item.
	OutputSchema schema.StepOutput
	// Data is the processed input containing expressions.
	Data any
	// DataSchema is the corresponding schema for the Data once the expressions are resolved.
	DataSchema schema.Type
	// Provider is the runnable step from the step provider that can be executed.
	Provider step.RunnableStep
}

// String provides an identifier for this DAG item constructed from the contents.
func (d DAGItem) String() string {
	switch d.Kind {
	case DAGItemKindInput:
		return "input"
	case DAGItemKindOutput:
		return fmt.Sprintf("outputs.%s", d.OutputID)
	default:
		if d.OutputID != "" {
			return GetOutputNodeID(d.StepID, d.StageID, d.OutputID)
		}
		if d.StageID != "" {
			return GetStageNodeID(d.StepID, d.StageID)
		}
		panic("DAG item for step without stage")
	}
}

// GetStageNodeID returns the DAG node ID for a stage.
func GetStageNodeID(stepID string, stageID string) string {
	return fmt.Sprintf("steps.%s.%s", stepID, stageID)
}

// GetOutputNodeID returns the DAG node ID for a stage output.
func GetOutputNodeID(stepID string, stageID string, outputID string) string {
	return fmt.Sprintf("steps.%s.%s.%s", stepID, stageID, outputID)
}

// ErrNoMorePossibleSteps indicates that the workflow has finished, but the output cannot be constructed.
type ErrNoMorePossibleSteps struct {
	dag dgraph.DirectedGraph[*DAGItem]
}

// Error returns an explanation on why the error happened.
func (e ErrNoMorePossibleSteps) Error() string {
	var outputsUnmetDependencies []string //nolint:prealloc
	for _, node := range e.dag.ListNodes() {
		if node.Item().Kind != DAGItemKindOutput {
			continue
		}
		var unmetDependencies []string
		inbound, err := node.ListInboundConnections()
		if err != nil {
			panic(fmt.Errorf("failed to fetch output node inbound dependencies (%w)", err))
		}
		for i := range inbound {
			unmetDependencies = append(unmetDependencies, i)
		}
		outputsUnmetDependencies = append(
			outputsUnmetDependencies,
			fmt.Sprintf("%s: %s", node.Item().OutputID, strings.Join(unmetDependencies, ", ")),
		)
	}
	return fmt.Sprintf(
		"no steps running, no more executable steps, cannot construct any output (outputs have the following dependencies: %s)",
		strings.Join(outputsUnmetDependencies, "; "),
	)
}

// ErrInvalidState indicates that the workflow failed due to an invalid state.
type ErrInvalidState struct {
	processingSteps int
	msg             string
}

// Error returns an explanation on why the error happened.
func (e ErrInvalidState) Error() string {
	return fmt.Sprintf("Workflow failed due to invalid state (%s). Processing steps: %d", e.msg, e.processingSteps)
}

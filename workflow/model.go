package workflow

import (
	"fmt"
	"regexp"

	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Workflow is the primary data structure describing workflows.
type Workflow struct {
	// Input describe the input schema for a workflow. These values can be referenced from expressions. The structure
	// must be a scope described in primitive types. This is done so later on a forward reference to a step input can
	// be used.
	Input any `json:"input"`
	// Steps contains the possible steps in this workflow. The data set must contain a valid step structure where the
	// inputs to stages may consist only of primitive types and expressions.
	Steps map[string]any `json:"steps"`
	// Output creates an actual output data structure (not a schema), which can contain expressions to construct the
	// output.
	Output any `json:"output"`
}

// getSchema returns the entire workflow schema.
func getSchema() *schema.TypedScopeSchema[*Workflow] {
	return schema.NewTypedScopeSchema[*Workflow](
		schema.NewStructMappedObjectSchema[*Workflow](
			"Workflow",
			map[string]*schema.PropertySchema{
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
						schema.PointerTo("Output data structure with expressions to pull in output data from steps."),
						nil,
					),
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
	// Input is the processed input containing expressions.
	Input any
	// InputSchema is the corresponding schema for the Input once the expressions are resolved.
	InputSchema schema.Type
	// Provider is the runnable step from the step provider that can be executed.
	Provider step.RunnableStep
}

// String provides an identifier for this DAG item constructed from the contents.
func (d DAGItem) String() string {
	switch d.Kind {
	case DAGItemKindInput:
		return "input"
	case DAGItemKindOutput:
		return "output"
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

package workflow

import (
	"regexp"

	"go.flow.arcalot.io/pluginsdk/schema"
)

// Workflow is the primary data structure describing workflows.
type Workflow struct {
	// Input describe the input schema for a workflow. These values can be referenced from expressions.
	Input *schema.ScopeSchema `json:"input"`
	// Steps contains the possible steps in this workflow. Steps are executed in order of their dependencies.
	Steps map[string]*PluginStep `json:"steps"`
	// Output creates an actual output data structure (not a schema), which can contain expressions to construct the
	// output.
	Output any `json:"output"`
}

// GetSchema returns the entire workflow schema.
func GetSchema() *schema.TypedScopeSchema[*Workflow] {
	return schema.NewTypedScopeSchema[*Workflow](
		schema.NewStructMappedObjectSchema[*Workflow](
			"Workflow",
			map[string]*schema.PropertySchema{
				"input": schema.NewPropertySchema(
					schema.DescribeScope(),
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
						getPluginStepSchema(),
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
					schema.NewAnySchema(),
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

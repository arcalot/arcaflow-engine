package workflow

import (
	"go.flow.arcalot.io/engine/internal/util"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// PluginStep describes a step that is tied to an external plugin.
type PluginStep struct {
	// Input is the input data structure for this step.
	Input any `json:"input"`

	// Plugin describes the plugin container image to be used.
	Plugin string `json:"plugin"`

	// Step is the step offered by the plugin to execute.
	Step string `json:"step"`

	// Deploy describes how the plugin should be deployed. The Type string describes which deployment utility should be
	// used.
	Deploy any `json:"deploy"`
}

func getPluginStepSchema() schema.Object { //nolint:funlen
	return schema.NewStructMappedObjectSchema[*PluginStep](
		"PluginStep",
		map[string]*schema.PropertySchema{
			"input": schema.NewPropertySchema(
				// We use an any schema here because it will need to evaluate expressions before applying the schema.
				schema.NewAnySchema(),
				schema.NewDisplayValue(
					schema.PointerTo("Input"),
					schema.PointerTo("Input data for this step."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(util.JSONEncode([]string{})),
				nil,
			),
			"plugin": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), nil),
				schema.NewDisplayValue(
					schema.PointerTo("Plugin"),
					schema.PointerTo("The plugin container image in fully qualified form."),
					nil,
				),
				true,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"step": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), nil),
				schema.NewDisplayValue(
					schema.PointerTo("Step"),
					schema.PointerTo("Step from the plugin to execute."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"deploy": schema.NewPropertySchema(
				// We use an any schema here because it will need to evaluate expressions before applying the schema.
				schema.NewAnySchema(),
				schema.NewDisplayValue(
					schema.PointerTo("Deployment"),
					schema.PointerTo("Deployment configuration for this plugin."),
					nil,
				),
				true,
				[]string{"plugin"},
				nil,
				nil,
				schema.PointerTo(util.JSONEncode(map[string]any{"type": "docker"})),
				nil,
			),
		},
	)
}

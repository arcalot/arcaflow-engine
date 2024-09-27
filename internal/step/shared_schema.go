package step

import "go.flow.arcalot.io/pluginsdk/schema"

// EnabledOutputSchema returns the schema for the enabled stage that is
// implemented in several step types.
func EnabledOutputSchema() *schema.StepOutputSchema {
	return schema.NewStepOutputSchema(
		schema.NewScopeSchema(
			schema.NewObjectSchema(
				"EnabledOutput",
				map[string]*schema.PropertySchema{
					"enabled": schema.NewPropertySchema(
						schema.NewBoolSchema(),
						schema.NewDisplayValue(
							schema.PointerTo("enabled"),
							schema.PointerTo("Whether the step was enabled"),
							nil),
						true,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
				},
			),
		),
		nil,
		false,
	)
}

// DisabledOutputSchema returns the schema for the disabled stage that is
// implemented in several step types.
func DisabledOutputSchema() *schema.StepOutputSchema {
	return schema.NewStepOutputSchema(
		schema.NewScopeSchema(
			schema.NewObjectSchema(
				"DisabledMessageOutput",
				map[string]*schema.PropertySchema{
					"message": schema.NewPropertySchema(
						schema.NewStringSchema(nil, nil, nil),
						schema.NewDisplayValue(
							schema.PointerTo("message"),
							schema.PointerTo("A human readable message stating that the step was disabled."),
							nil),
						true,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
				},
			),
		),
		nil,
		false,
	)
}

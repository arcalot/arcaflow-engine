package workflow_test

import (
	"go.arcalot.io/assert"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/schema"
	"testing"
)

var fullWorkflow = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 1
outputs:
  success:
    first_step_output: !expr $.steps.long_wait.outputs
outputSchema:
  success:
    schema:
      root: RootObjectOut
      objects: 
        RootObjectOut: 
          id: RootObjectOut
          properties:
            message:
              type:
                type_id: string`

func Test_SchemaWorkflow(t *testing.T) {
	logger := log.NewLogger(log.LevelDebug, log.NewTestWriter(t))
	stepRegistry := NewTestImplStepRegistry(logger, t)
	yamlConverter := workflow.NewYAMLConverter(stepRegistry)
	wf, err := yamlConverter.FromYAML([]byte(fullWorkflow))
	assert.NoError(t, err)

	outputIDExp := "success"
	outputSchemaRootID := "RootObjectOut"
	outputSchemaProperties := map[string]*schema.PropertySchema{
		"message": schema.NewPropertySchema(
			schema.NewStringSchema(schema.IntPointer(1), nil, nil),
			nil, false, nil, nil,
			nil, nil, nil)}
	rootObjectOut := schema.NewObjectSchema(outputSchemaRootID, outputSchemaProperties)
	stepOutputSchema := schema.NewStepOutputSchema(schema.NewScopeSchema(
		rootObjectOut), nil, false)
	workflowOutputSchema := map[string]*schema.StepOutputSchema{
		outputIDExp: stepOutputSchema,
	}
	assert.NoError(t, wf.OutputSchema["success"].ValidateCompatibility(workflowOutputSchema["success"]))
}

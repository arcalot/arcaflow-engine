package workflow_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/workflow"
	"testing"
)

// Test_SchemaWorkflow tests the workflow model schema's ability
// to validate the compatibility of a fully specified and valid
// workflow.
func Test_SchemaWorkflow(t *testing.T) {
	stepOutputSchemaExp := map[string]any{
		"schema": map[string]any{
			"root": "RootObjectOut",
			"objects": map[string]any{
				"RootObjectOut": map[string]any{
					"id": "RootObjectOut",
					"properties": map[string]any{
						"message": map[string]any{
							"type": map[string]any{
								"type_id": "string",
							}}}}}},
	}
	workflowSchemaInput := map[string]any{
		"version": "v0.2.0",
		"input": map[string]any{
			"root": "RootObject",
			"objects": map[string]any{
				"RootObject": map[string]any{
					"id":         "RootObject",
					"properties": map[string]any{}}},
		},
		"steps": map[string]any{
			"long_wait": map[string]any{
				"plugin": map[string]any{
					"src":             "n/a",
					"deployment_type": "builtin",
				},
				"step": "wait",
				"input": map[string]any{
					"wait_time_ms": 1}},
		},
		"outputs": map[string]any{
			"success": "!expr $.steps.long_wait.outputs",
		},
		"outputSchema": map[string]any{
			"success": stepOutputSchemaExp,
		},
	}
	workflowModelSchema := workflow.GetSchema()
	assert.NoError(t, workflowModelSchema.ValidateCompatibility(workflowSchemaInput))
	_, err := workflowModelSchema.UnserializeType(workflowSchemaInput)
	assert.NoError(t, err)
}

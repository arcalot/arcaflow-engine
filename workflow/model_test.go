package workflow_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/workflow"
	"testing"
)

var testVersionExp = "v0.2.0"
var inputExp = map[string]any{
	"root": "RootObject",
	"objects": map[string]any{
		"RootObject": map[string]any{
			"id":         "RootObject",
			"properties": map[string]any{}}},
}
var stepsExp = map[string]any{
	"long_wait": map[string]any{
		"plugin": map[string]any{
			"src":             "n/a",
			"deployment_type": "builtin",
		},
		"step": "wait",
		"input": map[string]any{
			"wait_time_ms": 1}},
}
var testExpectedOutputID = "success"
var outputsExp = map[string]any{
	testExpectedOutputID: "!expr $.steps.long_wait.outputs",
}
var outputSchemaRootID = "RootObjectOut"
var stepOutputSchemaInput = map[string]any{
	"schema": map[string]any{
		"root": outputSchemaRootID,
		"objects": map[string]any{
			outputSchemaRootID: map[string]any{
				"id": outputSchemaRootID,
				"properties": map[string]any{
					"message": map[string]any{
						"type": map[string]any{
							"type_id": "string",
						}}}}}},
}
var outputSchemaInput = map[string]any{
	testExpectedOutputID: stepOutputSchemaInput,
}
var workflowSchemaInput = map[string]any{
	"version":      testVersionExp,
	"input":        inputExp,
	"steps":        stepsExp,
	"outputs":      outputsExp,
	"outputSchema": outputSchemaInput,
}

// Test_SchemaWorkflow tests the workflow model schema's ability
// to validate the compatibility of a fully specified and valid
// workflow.
func Test_SchemaWorkflow_ValidateCompatibility(t *testing.T) {
	workflowModelSchema := workflow.GetSchema()
	assert.NoError(t, workflowModelSchema.ValidateCompatibility(workflowSchemaInput))
}

// Test_SchemaWorkflow tests the workflow model schema's ability
// to unserialize a fully specified and valid workflow.
func Test_SchemaWorkflow_UnserializeType(t *testing.T) {
	workflowModelSchema := workflow.GetSchema()
	_, err := workflowModelSchema.UnserializeType(workflowSchemaInput)
	assert.NoError(t, err)
}

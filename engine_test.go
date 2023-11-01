package engine_test

import (
	"context"
	"errors"
	"testing"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/workflow"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/config"
)

func TestEngineWorkflow_ParseVersion(t *testing.T) {
	_, err := engine.SupportedVersion("v0.2.0")
	assert.NoError(t, err)

	// test unsupported version
	_, err = engine.SupportedVersion("v0.1000.0")
	assert.Error(t, err)
}

func createTestEngine(t *testing.T) engine.WorkflowEngine {
	cfg := config.Default()
	cfg.Log.T = t
	cfg.Log.Level = log.LevelDebug
	cfg.Log.Destination = log.DestinationTest
	flow, err := engine.New(
		cfg,
	)
	assert.NoError(t, err)
	return flow
}

func TestNoWorkflowFile(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	if !errors.Is(err, engine.ErrNoWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestEmptyWorkflowFile(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": {},
		},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	if !errors.Is(err, workflow.ErrEmptyWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestInvalidYAML(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`: foo
  bar`),
		},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	var invalidYAML *workflow.ErrInvalidWorkflowYAML
	if !errors.As(err, &invalidYAML) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestInvalidWorkflow(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`test: Hello world!`),
		},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	var invalidYAML *workflow.ErrInvalidWorkflow
	if !errors.As(err, &invalidYAML) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestEmptySteps(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`version: v0.2.0
output: []
steps: []`),
		},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
}

func TestNoSteps(t *testing.T) {
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`version: v0.2.0
output: []`),
		},
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
}

func TestE2E(t *testing.T) {
	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`name: Arca Lot`),
		map[string][]byte{
			"workflow.yaml": []byte(`version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        name:
          type:
            type_id: string
steps:
  example:
    plugin: 
      src: quay.io/arcalot/arcaflow-plugin-template-python:0.2.1
      deployment_type: image
    input:
      name: !expr $.input.name
output:
  message: !expr $.steps.example.outputs.success.message`),
		},
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

func TestE2EMultipleOutputs(t *testing.T) {
	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`name: Arca Lot`),
		map[string][]byte{
			"workflow.yaml": []byte(`version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        name:
          type:
            type_id: string
steps:
  example:
    plugin: 
      src: quay.io/arcalot/arcaflow-plugin-template-python:0.2.1
      deployment_type: image
    input:
      name: !expr $.input.name
outputs:
  success:
    message: !expr $.steps.example.outputs.success.message`),
		},
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

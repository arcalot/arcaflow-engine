package engine_test

import (
	"context"
	"errors"
	"go.flow.arcalot.io/engine/loadfile"
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
	fileCache := loadfile.NewFileCache("", map[string][]byte{})
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	if !errors.Is(err, engine.ErrNoWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestEmptyWorkflowFile(t *testing.T) {
	fileCache := loadfile.NewFileCache("",
		map[string][]byte{
			"workflow.yaml": {},
		})
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
	if !errors.Is(err, workflow.ErrEmptyWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestInvalidYAML(t *testing.T) {
	content := map[string][]byte{
		"workflow.yaml": []byte(`: foo
  bar`),
	}
	fileCache := loadfile.NewFileCache("", content)
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
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
	content := map[string][]byte{
		"workflow.yaml": []byte(`test: Hello world!`),
	}
	fileCache := loadfile.NewFileCache("", content)
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
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
	content := map[string][]byte{
		"workflow.yaml": []byte(`version: v0.2.0
output: []
steps: []`),
	}
	fileCache := loadfile.NewFileCache("", content)
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
}

func TestNoSteps(t *testing.T) {
	content := map[string][]byte{
		"workflow.yaml": []byte(`version: v0.2.0
output: []`),
	}
	fileCache := loadfile.NewFileCache("", content)
	_, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		fileCache,
		"",
	)
	assert.Error(t, err)
	assert.Equals(t, outputError, true)
}

func TestE2E(t *testing.T) {
	content := map[string][]byte{
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
	}
	fileCache := loadfile.NewFileCache("", content)
	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`name: Arca Lot`),
		fileCache,
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

func TestE2EMultipleOutputs(t *testing.T) {
	content := map[string][]byte{
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
	}
	fileCache := loadfile.NewFileCache("", content)
	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`name: Arca Lot`),
		fileCache,
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

func Test_ParseSubworkflows(t *testing.T) {
	fileCache, err := loadfile.NewFileCacheUsingContext(
		"fixtures/test-subworkflow",
		map[string]string{
			"workflow": "test-workflow.yaml",
		})
	assert.NoError(t, err)
	assert.NoError(t, fileCache.LoadContext())
	outputID, _, outputError, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`{}`),
		fileCache,
		"workflow",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
}

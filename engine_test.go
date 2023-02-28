package engine_test

import (
	"context"
	"errors"
	log "go.arcalot.io/log/v2"
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/config"
)

func createTestEngine(t *testing.T) engine.WorkflowEngine {
	cfg := config.Default()
	cfg.Log.T = t
	cfg.Log.Level = log.LevelDebug
	cfg.Log.Destination = log.DestinationTest
	flow, err := engine.New(
		cfg,
		engine.DefaultDeployerRegistry,
	)
	assert.NoError(t, err)
	return flow
}

func TestNoWorkflowFile(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{},
		"",
	)
	assert.Error(t, err)
	if !errors.Is(err, engine.ErrNoWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestEmptyWorkflowFile(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": {},
		},
		"",
	)
	assert.Error(t, err)
	if !errors.Is(err, engine.ErrEmptyWorkflowFile) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestInvalidYAML(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`: foo
  bar`),
		},
		"",
	)
	assert.Error(t, err)
	var invalidYAML engine.ErrInvalidWorkflowYAML
	if !errors.As(err, &invalidYAML) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestInvalidWorkflow(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`test: Hello world!`),
		},
		"",
	)
	assert.Error(t, err)
	var invalidYAML engine.ErrInvalidWorkflow
	if !errors.As(err, &invalidYAML) {
		t.Fatalf("Incorrect error returned.")
	}
}

func TestEmptySteps(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`output: []
steps: []`),
		},
		"",
	)
	assert.Error(t, err)
}

func TestNoSteps(t *testing.T) {
	_, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		nil,
		map[string][]byte{
			"workflow.yaml": []byte(`output: []`),
		},
		"",
	)
	assert.Error(t, err)
}

func TestE2E(t *testing.T) {
	outputData, err := createTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`name: Arca Lot`),
		map[string][]byte{
			"workflow.yaml": []byte(`input:
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
    plugin: ghcr.io/janosdebugs/arcaflow-example-plugin
    input:
      name: !expr $.input.name
output:
  message: !expr $.steps.example.outputs.success.message`),
		},
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

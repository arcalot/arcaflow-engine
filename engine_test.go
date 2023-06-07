package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine"
	"go.flow.arcalot.io/engine/workflow"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/config"
)

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
			"workflow.yaml": []byte(`output: []
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
			"workflow.yaml": []byte(`output: []`),
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
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
}

//func TestE2EMultipleOutputs(t *testing.T) {
//	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
//		context.Background(),
//		[]byte(`name: Arca Lot`),
//		map[string][]byte{
//			"workflow.yaml": []byte(`input:
//  root: RootObject
//  objects:
//    RootObject:
//      id: RootObject
//      properties:
//        name:
//          type:
//            type_id: string
//steps:
//  example:
//    plugin: ghcr.io/janosdebugs/arcaflow-example-plugin
//    input:
//      name: !expr $.input.name
//outputs:
//  success:
//    message: !expr $.steps.example.outputs.success.message`),
//		},
//		"",
//	)
//	assert.NoError(t, err)
//	assert.Equals(t, outputError, false)
//	assert.Equals(t, outputID, "success")
//	assert.Equals(t, outputData.(map[any]any), map[any]any{"message": "Hello, Arca Lot!"})
//}

func TestE2EMultipleSteps(t *testing.T) {
	outputID, outputData, outputError, err := createTestEngine(t).RunWorkflow(
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
  example2:
    plugin: ghcr.io/janosdebugs/arcaflow-example-plugin
    input:
      name: !expr $.input.name
output:
  message: !expr $.steps.example.outputs.success.message
  message2: !expr $.steps.example2.outputs.success.message`),
		},
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"message":  "Hello, Arca Lot!",
		"message2": "Hello, Arca Lot!"})
}

func TestParallelSteps(t *testing.T) {
	outputID, outputData, outputError, err := createNetworkedTestEngine(t).RunWorkflow(
		context.Background(),
		[]byte(`{}`),
		map[string][]byte{
			"workflow.yaml": []byte(`input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  long_wait:
    plugin: quay.io/joconnel/arcaflow-plugin-wait
    step: wait
    input:
      seconds: 10
  medium_wait:
    plugin: quay.io/joconnel/arcaflow-plugin-wait
    step: wait
    input:
      seconds: 8
  short_wait:
    plugin: quay.io/joconnel/arcaflow-plugin-wait
    step: wait
    input:
      seconds: 1
outputs:
  a:
    b: !expr $.steps.medium_wait.outputs.success`),
		},
		"",
	)
	assert.NoError(t, err)
	assert.Equals(t, outputError, false)
	assert.Equals(t, outputID, "a")
	//assert.Equals(t, outputData.(map[any]any), map[any]any{
	//	"message":  "Hello, Arca Lot!",
	//	"message2": "Hello, Arca Lot!"})
	fmt.Printf("%s\n", outputData)
}

func createNetworkedTestEngine(t *testing.T) engine.WorkflowEngine {
	cfg := &config.Config{
		LocalDeployer: map[any]any{
			//LocalDeployer: map[any]any{"type": "docker"},
			"type": "podman",
			//"type": "docker",
			"deployment": map[any]any{
				"host": map[any]any{
					"NetworkMode": "host",
				},
				"imagePullPolicy": "Always",
			}},
	}
	cfg.Log.T = t
	cfg.Log.Level = log.LevelDebug
	cfg.Log.Destination = log.DestinationTest
	flow, err := engine.New(
		cfg,
	)
	assert.NoError(t, err)
	return flow
}

package workflow_test

import (
	"context"
	"fmt"
	"go.flow.arcalot.io/engine/config"
	"testing"

	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

var sharedInputWorkflowYAML = `---
apiVersion: 0.1.0
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
  say_hi:
    kind: dummy
    name: !expr $.input.name
    nickname: !expr $.input.name
output:
  message: !expr $.steps.say_hi.greet.success.message
  message2: !expr $.steps.say_hi.greet.success.message
`

// The special cases this test case tests include one input going into two step-inputs,
// and one step-output going into two step-outputs.
// These cause duplicate connections to be made, which need to be handled properly.
func TestSharedInput(t *testing.T) {
	logger := log.NewLogger(log.LevelDebug, log.NewTestWriter(t))
	stepRegistry := lang.Must2(registry.New(
		dummy.New(),
	))

	executor, err := workflow.NewExecutor(logger, &config.Config{}, stepRegistry)
	if err != nil {
		t.Fatalf("Failed to create Executor, %e", err)
	}

	yamlConverter := workflow.NewYAMLConverter(stepRegistry)
	decodedWorkflow, err := yamlConverter.FromYAML([]byte(sharedInputWorkflowYAML))
	if err != nil {
		t.Fatalf("Failed to load workflow from YAML, %e", err)
	}

	preparedWorkflow, err := executor.Prepare(decodedWorkflow, nil)
	if err != nil {
		t.Fatalf("Failed to prepare workflow, %e", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outputID, outputData, err := preparedWorkflow.Execute(ctx, map[string]any{
		"name": "Arca Lot",
	})
	if err != nil {
		t.Fatalf("Error while executing workflow, %e", err)
	}
	fmt.Printf("%s: %s\n", outputID, outputData.(map[any]any)["message"])
	// Output: success: Hello Arca Lot!
}

package workflow_test

import (
	"context"
	"fmt"
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/deployer"
	deployerregistry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	testimpl "go.flow.arcalot.io/testdeployer"
	"testing"

	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	stepregistry "go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

func getTestImplPreparedWorkflow(t *testing.T, workflowDefinition string) (workflow.ExecutableWorkflow, error) {
	logger := log.NewLogger(log.LevelDebug, log.NewTestWriter(t))
	cfg := &config.Config{
		LoggedOutputConfigs: map[string]*config.StepOutputLogConfig{
			"terminated_early": {
				LogLevel: log.LevelError,
			},
		},
	}
	stepRegistry := NewTestImplStepRegistry(logger, t)

	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
	))
	wf := assert.NoErrorR[*workflow.Workflow](t)(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(workflowDefinition)))
	return executor.Prepare(wf, map[string][]byte{})
}

func getDummyDeployerPreparedWorkflow(t *testing.T, workflowDefinition string) (workflow.ExecutableWorkflow, error) {
	logger := log.NewLogger(log.LevelDebug, log.NewTestWriter(t))
	cfg := &config.Config{}
	stepRegistry := assert.NoErrorR[step.Registry](t)(stepregistry.New(
		dummy.New(),
	))
	executor := assert.NoErrorR[workflow.Executor](t)(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
	))
	wf := assert.NoErrorR[*workflow.Workflow](t)(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(workflowDefinition)))
	return executor.Prepare(wf, map[string][]byte{})
}

func NewTestImplStepRegistry(
	logger log.Logger,
	t *testing.T,
) step.Registry {
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"deployer_id": "test-impl",
			"deploy_time": "0",
		}),
	)
	return assert.NoErrorR[step.Registry](t)(stepregistry.New(
		pluginProvider,
	))
}

var sharedInputWorkflowYAML = `---
version: v0.1.0
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
    # Both name and nickname reference the same variable
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
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getDummyDeployerPreparedWorkflow(t, sharedInputWorkflowYAML),
	)
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

var missingInputWorkflowDefinition1 = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  incomplete_wait:
    plugin: "n/a"
    step: wait
    # Missing input
outputs:
  a:
    b: !expr $.steps.incomplete_wait.outputs
`

var missingInputWorkflowDefinition2 = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  say_hi:
    kind: dummy
    # Missing name
outputs:
  a:
    b: !expr $.steps.say_hi.greet
`

func TestMissingInput(t *testing.T) {
	// For this test, a workflow's step will be missing its inputs.
	_, err := getTestImplPreparedWorkflow(t, missingInputWorkflowDefinition1)
	assert.Error(t, err)

	_, err = getDummyDeployerPreparedWorkflow(t, missingInputWorkflowDefinition2)
	assert.Error(t, err)
}

var mismatchedStepInputTypesWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  wait_1:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 0
  wait_2:
    plugin: "n/a"
    step: wait
    input:
      # Should fail during preparation, due to message being a string, and wait_time_ms expecting an int
      wait_time_ms: !expr $.steps.wait_1.outputs.success.message
outputs:
  a:
    b: !expr $.steps.wait_2.outputs
`

func TestMismatchedStepInputTypes(t *testing.T) {
	_, err := getTestImplPreparedWorkflow(t, mismatchedStepInputTypesWorkflowDefinition)
	assert.Error(t, err)
}

var mismatchedInputTypesWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        a:
          display:
            description: "Just for testing"
            name: "a"
          required: true
          type:
            type_id: string
steps:
  wait_1:
    plugin: "n/a"
    step: wait
    input:
      # This is trying to put a string into an int field
      wait_time_ms: !expr $.input.a
outputs:
  a:
    b: !expr $.steps.wait_1.outputs
`

func TestMismatchedInputTypes(t *testing.T) {
	_, err := getTestImplPreparedWorkflow(t, mismatchedInputTypesWorkflowDefinition)
	assert.Error(t, err)
}

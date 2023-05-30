package workflow_test

import (
	"context"
	"errors"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	testimpl "go.flow.arcalot.io/testdeployer"
	"testing"

	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	deployerregistry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	stepregistry "go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

var badWorkflowDefinition = `
input:
  root: name
  objects:
    name:
      id: name
      properties:
        name:
          type:
            type_id: string
steps:
  say_hi:
    kind: dummy
    name: !expr $.input.name
output:
  thiswillfail: !expr $.steps.say_hi.greet.error.reason
`

func TestOutputFailed(t *testing.T) {
	logConfig := log.Config{
		Level:       log.LevelError,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	cfg := &config.Config{
		Log: logConfig,
	}
	stepRegistry := lang.Must2(stepregistry.New(
		dummy.New(),
	))
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(badWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	_, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{"name": "Arca Lot"})
	assert.Nil(t, outputData)
	assert.Error(t, err)
	var typedError *workflow.ErrNoMorePossibleSteps
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T", err)
	}
}

var stepCancellationWorkflowDefinition = `
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  long_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 2000
    stop_if: !expr $.steps.short_wait.outputs
  short_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 0
outputs:
  a:
    cancelled_step_output: !expr $.steps.long_wait.outputs
`

func NewTestImplStepRegistry(
	logger log.Logger,
	t *testing.T,
) step.Registry {
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"type":        "test-impl",
			"deploy_time": "0",
		}),
	)
	return assert.NoErrorR[step.Registry](t)(stepregistry.New(
		pluginProvider,
	))
}

func TestStepCancellation(t *testing.T) {
	// For this test, a simple workflow will run wait steps, with one that's
	// supposed to be stopped when the first stops.
	// The long one will be long enough that there is no reasonable way
	// for it to finish before the first step.
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	logConfig := log.Config{
		Level:       log.LevelInfo,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	cfg := &config.Config{
		Log: logConfig,
	}
	stepRegistry := NewTestImplStepRegistry(logger, t)

	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(stepCancellationWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	stepResult := outputData.(map[interface{}]interface{})["cancelled_step_output"]
	assert.NotNil(t, stepResult)
	stepResultCancelledEarly := stepResult.(map[string]interface{})["cancelled_early"]
	assert.NotNil(t, stepResultCancelledEarly)
}

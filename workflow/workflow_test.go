package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	testimpl "go.flow.arcalot.io/testdeployer"

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

var waitForSerialWorkflowDefinition = `
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  first_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 500
  second_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 500
    wait_for: !expr $.steps.first_wait.outputs.success
outputs:
  success:
    first_step_output: !expr $.steps.first_wait.outputs
    second_step_output: !expr $.steps.second_wait.outputs
`

func TestWaitForSerial(t *testing.T) {
	// For this test, a workflow runs two steps, where each step runs a wait step for 5s
	// The second wait step waits for the first to succeed after which it runs
	// Due to the wait for condition, the steps will execute serially
	// The total execution time for this test function should be greater than 10seconds
	// as each step runs for 5s and are run serially
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	startTime := time.Now()
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
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(waitForSerialWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
	stepResult := outputData.(map[interface{}]interface{})["first_step_output"]
	assert.NotNil(t, stepResult)
	stepResultWaitFor := stepResult.(map[string]interface{})["success"]
	assert.NotNil(t, stepResultWaitFor)
	stepResult2 := outputData.(map[interface{}]interface{})["second_step_output"]
	assert.NotNil(t, stepResult2)
	stepResultWaitFor2 := stepResult.(map[string]interface{})["success"]
	assert.NotNil(t, stepResultWaitFor2)

	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	var waitSuccess bool
	if duration >= 1*time.Second {
		waitSuccess = true
		t.Logf("Test execution time is greater than 1 second, steps are running serially due to the wait_for condition.")
	} else {
		waitSuccess = false
		t.Logf("Test execution time is lesser than 1 seconds, steps are not running serially.")
	}
	assert.Equals(t, waitSuccess, true)
}

var waitForParallelWorkflowDefinition = `
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  first_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 500
  second_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 500
    wait_for: !expr $.steps.first_wait.outputs.success
  third_wait:
    plugin: "n/a"
    step: wait
    input:
      wait_time_ms: 500
    wait_for: !expr $.steps.first_wait.outputs.success
outputs:
  success:
    third_step_output: !expr $.steps.third_wait.outputs
    second_step_output: !expr $.steps.second_wait.outputs
`

func TestWaitForParallel(t *testing.T) {
	// For this test, a workflow runs three steps, where each step runs a wait step for 5s
	// The second and third wait steps wait for the first to succeed after which they both run in parallel
	// The total execution time for this test function should be greater than 5s but lesser than 15s
	// as the first step runs for 5s and other two steps run in parallel after the first succeeds
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	startTime := time.Now()
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
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(waitForParallelWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
	stepResult2 := outputData.(map[interface{}]interface{})["second_step_output"]
	assert.NotNil(t, stepResult2)
	stepResult3 := outputData.(map[interface{}]interface{})["third_step_output"]
	assert.NotNil(t, stepResult3)
	t.Log(stepResult3)

	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	var waitSuccess bool
	if duration > 1*time.Second && duration < 2*time.Second {
		waitSuccess = true
		t.Logf("Steps second_wait and third_wait are running in parallel after waiting for the first_wait step.")
	} else {
		waitSuccess = false
		t.Logf("Steps second_wait and third_wait are not running in parallel.")
	}
	assert.Equals(t, waitSuccess, true)
}

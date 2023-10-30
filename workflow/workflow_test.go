package workflow_test

import (
	"context"
	"errors"
	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
	"testing"
	"time"

	"go.flow.arcalot.io/engine/workflow"
)

var badWorkflowDefinition = `
version: v0.1.0
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
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getDummyDeployerPreparedWorkflow(t, badWorkflowDefinition),
	)
	_, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{"name": "Arca Lot"})
	assert.Nil(t, outputData)
	assert.Error(t, err)
	var typedError *workflow.ErrNoMorePossibleSteps
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T", err)
	}
}

var stepCancellationWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2000
    stop_if: !expr $.steps.short_wait.outputs
  short_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      # It needs to be long enough for it to ensure that long_wait is in a running state.
      # The other case will be tested separately.
      wait_time_ms: 20
outputs:
  a:
    cancelled_step_output: !expr $.steps.long_wait.outputs
`

func TestStepCancellation(t *testing.T) {
	// For this test, a simple workflow will run wait steps, with one that's
	// supposed to be stopped when the first stops.
	// The long one will be long enough that there is no reasonable way
	// for it to finish before the first step.
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, stepCancellationWorkflowDefinition),
	)
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	stepResult := assert.MapContainsKeyAny(t, "cancelled_step_output", outputData.(map[any]any))
	assert.MapContainsKey(t, "cancelled_early", stepResult.(map[string]any))
}

var earlyStepCancellationWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  # This one needs to run longer than the total time expected of all the other steps, with
  # a large enough difference to prevent timing errors breaking the test.
  end_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 80
  # Delay needs to be delayed long enough to ensure that last_step isn't running when it's cancelled by short_wait
  delay:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 50
  last_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    # Delay it so it doesn't run, and gets cancelled before deployment.
    wait_for: !expr $.steps.delay.outputs
    # You can verify that this test works by commenting out this line. It should fail.
    stop_if: !expr $.steps.short_wait.outputs
  short_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      # End the test quickly.
      wait_time_ms: 0
outputs:
  # If not properly cancelled, fail_case will have output.
  fail_case:
    unattainable: !expr $.steps.last_step.outputs
  correct_case:
    a: !expr $.steps.end_wait.outputs
`

func TestEarlyStepCancellation(t *testing.T) {
	// For this test, a simple workflow will run wait steps, with the workflow
	// The long one will be long enough that there is no reasonable way
	// for it to finish before the first step.
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, earlyStepCancellationWorkflowDefinition),
	)
	startTime := time.Now() // Right before execute to not include pre-processing time.
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	// A nil value means the output could not be constructed, which is intended due to us cancelling the step it depends on.
	// If it's not nil, that means the step didn't get cancelled.
	assert.NoError(t, err)
	assert.Equals(t, outputID, "correct_case")
	// All steps that can result in output are 0 ms, so just leave some time for processing.
	assert.LessThan(t, duration.Milliseconds(), 200)
}

var deploymentStepCancellationWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  # This one needs to run longer than the total time expected of all the other steps, with
  # a large enough difference to prevent timing errors breaking the test.
  end_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 100
  step_to_cancel:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    # You can verify that this test works by commenting out this line. It should fail.
    stop_if: !expr $.steps.short_wait.outputs
    # Delay needs to be delayed long enough to ensure that it's in a deploy state when it's cancelled by short_wait
    deploy:
      deployer_id: "test-impl"
      deploy_time: 50 # 50 ms 
  short_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      # End the test quickly.
      wait_time_ms: 0
outputs:
  # If not properly cancelled, fail_case will have output.
  fail_case:
    unattainable: !expr $.steps.step_to_cancel.outputs
  correct_case:
    a: !expr $.steps.end_wait.outputs
`

func TestDeploymentStepCancellation(t *testing.T) {
	// For this test, a simple workflow will run wait steps, with the workflow
	// The long one will be long enough that there is no reasonable way
	// for it to finish before the first step.
	// The test double deployer will be used for this test, as we
	// need a deployer to test the plugin step provider.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, deploymentStepCancellationWorkflowDefinition),
	)
	startTime := time.Now() // Right before execute to not include pre-processing time.
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	// A nil value means the output could not be constructed, which is intended due to us cancelling the step it depends on.
	// If it's not nil, that means the step didn't get cancelled.
	assert.NoError(t, err)
	assert.Equals(t, outputID, "correct_case")
	// All steps that can result in output are 0 ms, so just leave some time for processing.
	assert.LessThan(t, duration.Milliseconds(), 200)
}

var simpleValidLiteralInputWaitWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  wait_1:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
outputs:
  a:
    b: !expr $.steps.wait_1.outputs
`

func TestSimpleValidWaitWorkflow(t *testing.T) {
	// Just a single wait
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, simpleValidLiteralInputWaitWorkflowDefinition),
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
}

var waitForSerialWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  first_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      # Note: 5ms left only a 2.5ms margin for error. 10ms left almost 6ms. So 10ms min is recommended.
      wait_time_ms: 10
  second_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 10
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
	startTime := time.Now() // Right before execute to not include pre-processing time.
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
	if duration >= 20*time.Millisecond {
		t.Logf("Test execution time is greater than 20 milliseconds; steps are correctly running serially due to the wait_for condition.")
	} else {
		t.Fatalf("Test execution time is less than 20 milliseconds; steps are not running serially.")
	}
}

var missingInputsFailedDeploymentWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  wait_1:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    deploy:
      deployer_id: "test-impl"
      #deploy_time: 20000 # 10 ms
      deploy_succeed: false
  wait_2:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    wait_for: !expr $.steps.wait_1.outputs.success
    input:
      wait_time_ms: 0
outputs:
  a:
    b: !expr $.steps.wait_2.outputs
`

func TestMissingInputsFailedDeployment(t *testing.T) {
	// For this test, the workflow should fail, not deadlock, due to no inputs possible.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, missingInputsFailedDeploymentWorkflowDefinition),
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Equals(t, outputID, "")
}

var missingInputsWrongOutputWorkflowDefinition = `
version: v0.1.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  wait_1:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
  wait_2:
    
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    # No stop_if, so this shouldn't happen.
    wait_for: !expr $.steps.wait_1.outputs.cancelled_early
    input:
      wait_time_ms: 0
outputs:
  a:
    b: !expr $.steps.wait_2.outputs
`

func TestMissingInputsWrongOutput(t *testing.T) {
	// For this test, the workflow should fail, not deadlock, due to no inputs possible.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, missingInputsWrongOutputWorkflowDefinition),
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Equals(t, outputID, "")
}

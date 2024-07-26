package workflow_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	deployerregistry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/builtinfunctions"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/foreach"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	stepregistry "go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/internal/util"
	"go.flow.arcalot.io/pluginsdk/schema"
	testimpl "go.flow.arcalot.io/testdeployer"
	"os"
	"testing"
	"time"

	"go.flow.arcalot.io/engine/workflow"
)

var badWorkflowDefinition = `
version: v0.2.0
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

	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
}

var simpleValidLiteralInputWaitWorkflowDefinition = `
version: v0.2.0
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

var stepCancellationWorkflowDefinition = `
version: v0.2.0
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
      wait_time_ms: 5
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
version: v0.2.0
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
      wait_time_ms: 10
  # Delay needs to be delayed long enough to ensure that last_step isn't running when it's cancelled by short_wait
  delay:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 5
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
version: v0.2.0
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
      wait_time_ms: 20
  step_to_cancel:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 1000
    # You can verify that this test works by commenting out this line. It should fail.
    stop_if: !expr $.steps.short_wait.outputs
    # Delay needs to be delayed long enough to ensure that it's in a deploy state when it's cancelled by short_wait
    deploy:
      deployer_name: "test-impl"
      deploy_time: 10 # ms 
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

func TestWithDoubleSerializationDetection(t *testing.T) {
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, simpleValidLiteralInputWaitWorkflowDefinition),
	)
	// First, get the root object
	inputSchema := preparedWorkflow.Input()
	rootObject := inputSchema.RootObject()
	type testIterType struct {
		defaultSpec *string
		input       map[string]any
	}
	testIter := []testIterType{
		// No default specified; input provided
		{
			nil,
			map[string]any{"error_detector": "original input"},
		},
		// Default specified; input provided (overrides default)
		{
			schema.PointerTo[string]("default"),
			map[string]any{"error_detector": "original input"},
		},
		// Default specified; input omitted (default value used)
		{
			schema.PointerTo[string]("default"),
			map[string]any{},
		},
	}
	for _, testData := range testIter {
		errorDetect := util.NewInvalidSerializationDetectorSchema()
		// Inject the error detector into the object
		rootObject.PropertiesValue["error_detector"] = schema.NewPropertySchema(
			errorDetect,
			nil,
			true,
			nil,
			nil,
			nil,
			testData.defaultSpec,
			nil,
		)
		outputID, _, err := preparedWorkflow.Execute(context.Background(), testData.input)
		assert.NoError(t, err)
		assert.Equals(t, outputID, "a")
		// Confirm that, while we did no double-unserializations or double-serializations,
		// we did do at least one single one.
		assert.Equals(t, errorDetect.SerializeCnt+errorDetect.UnserializeCnt > 0, true)
	}
}

var waitForSerialWorkflowDefinition = `
version: v0.2.0
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
		builtinfunctions.GetFunctions(),
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

var waitForStartedWorkflowDefinition = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  pre_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2
  first_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2
    wait_for: !expr $.steps.pre_wait.outputs
  second_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2
    wait_for: !expr $.steps.first_wait.starting.started
outputs:
  success:
    #first_step_output: !expr $.steps.first_wait.outputs
    second_step_output: !expr $.steps.second_wait.outputs
`

func TestWaitForStarted(t *testing.T) {
	// For this test, the second step is depending on a step's running state, which is not a finished output node.
	logConfig := log.Config{
		Level:       log.LevelDebug,
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
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(waitForStartedWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
}

var waitForSerialForeachWf = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject 
      properties: {}
steps:
  second_wait:
    wait_for: !expr $.steps.first_wait.outputs.success
    kind: foreach
    items: 
    - wait_time_ms: 10
    workflow: subworkflow.yaml
  first_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 10
outputs:
  success:
    first_step_output: !expr $.steps.first_wait.outputs
    second_step_output: !expr $.steps.second_wait.outputs
`

var waitForSerialForeachSubwf = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        wait_time_ms:
          type:
            type_id: integer
steps:
  wait_1:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: !expr $.input.wait_time_ms
outputs:
  success:
    b: !expr $.steps.wait_1.outputs   
`

func TestWaitForSerial_Foreach(t *testing.T) {
	// This test highlights a lack of observability in this part of
	// Arcaflow's engine.
	// For this test, a workflow runs two steps, where each step runs a wait
	// step for 10 ms. The second wait step waits for the first to succeed
	// after which it runs. Due to the wait for condition, the steps will
	// execute serially. The total execution time for this test function
	// should be greater than 10 ms as the first step and the foreach steps
	// run serially.

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
	factories := workflowFactory{
		config: cfg,
	}
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"builtin": map[string]any{
				"deployer_name": "test-impl",
				"deploy_time":   "0",
			},
		}),
	)
	stepRegistry, err := stepregistry.New(
		pluginProvider,
		lang.Must2(foreach.New(logger, factories.createYAMLParser, factories.createWorkflow)),
	)
	assert.NoError(t, err)

	factories.stepRegistry = stepRegistry
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(waitForSerialForeachWf)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{
		"subworkflow.yaml": []byte(waitForSerialForeachSubwf),
	}))
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

var waitForStartedForeachWf = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject 
      properties: {}
steps:
  pre_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2
  second_wait:
    wait_for: !expr $.steps.first_wait.starting.started
    kind: foreach
    items: 
    - wait_time_ms: 2
    workflow: subworkflow.yaml
  first_wait:
    wait_for: !expr $.steps.pre_wait.outputs
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 2
outputs:
  success:
    first_step_output: !expr $.steps.first_wait.outputs
    second_step_output: !expr $.steps.second_wait.outputs
`

func TestWaitForStarted_Foreach(t *testing.T) {
	// This test highlights a lack of observability in this part of
	// Arcaflow's engine.
	// For this test, the second wait step depends on the first wait
	// step's running state being started.

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
	factories := workflowFactory{
		config: cfg,
	}
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"builtin": map[string]any{
				"deployer_name": "test-impl",
				"deploy_time":   "0",
			},
		}),
	)
	stepRegistry, err := stepregistry.New(
		pluginProvider,
		lang.Must2(foreach.New(logger, factories.createYAMLParser, factories.createWorkflow)),
	)
	assert.NoError(t, err)

	factories.stepRegistry = stepRegistry
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(waitForStartedForeachWf)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{
		"subworkflow.yaml": []byte(waitForSerialForeachSubwf),
	}))

	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
}

type workflowFactory struct {
	stepRegistry step.Registry
	config       *config.Config
}

func (f *workflowFactory) createYAMLParser() (workflow.YAMLConverter, error) {
	stepR := f.stepRegistry
	if stepR == nil {
		return nil, fmt.Errorf("YAML converter not available yet, please call the factory function after the engine has initialized")
	}
	return workflow.NewYAMLConverter(stepR), nil
}

func (f *workflowFactory) createWorkflow(logger log.Logger) (workflow.Executor, error) {
	stepR := f.stepRegistry
	if stepR == nil {
		return nil, fmt.Errorf("YAML converter not available yet, please call the factory function after the engine has initialized")
	}
	return workflow.NewExecutor(logger, f.config, stepR, builtinfunctions.GetFunctions())
}

var missingInputsFailedDeploymentWorkflowDefinition = `
version: v0.2.0
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
      deployer_name: "test-impl"
      deploy_succeed: false # This step will fail due to this.
  wait_2:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    # This step waits for the failing step here.
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
	t.Logf("Test output: %s", err.Error())
	assert.Equals(t, outputID, "")
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
}

var missingInputsWrongOutputWorkflowDefinition = `
version: v0.2.0
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

var fourSecWaitWorkflowDefinition = `
version: v0.2.0
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
    closure_wait_timeout: 5000
    input:
      wait_time_ms: 4000
outputs:
  success:
    first_step_output: !expr $.steps.long_wait.outputs
`

func TestEarlyContextCancellation(t *testing.T) {
	// Test to ensure the workflow aborts when instructed to.
	// The wait step should exit gracefully when the workflow is cancelled.
	logConfig := log.Config{
		Level:       log.LevelDebug,
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
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(fourSecWaitWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	// Cancel the context after 30 ms to simulate cancellation with ctrl-c.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	startTime := time.Now() // Right before execute to not include pre-processing time.
	//nolint:dogsled
	_, _, _ = preparedWorkflow.Execute(ctx, map[string]any{})
	cancel()

	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	if duration >= 1000*time.Millisecond {
		t.Fatalf("Test execution time is greater than 100 milliseconds; Is the workflow properly cancelling?")
	}
}

var justMathWorkflowDefinition = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        a:
         type:
           type_id: integer
        b:
         type:
           type_id: integer
steps:
  no_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
outputs:
  success:
    result: !expr intToString($.input.a + $.input.b) + " and " + $.steps.no_wait.outputs.success.message
`

func TestWorkflowWithMathAndFunctions(t *testing.T) {
	// For this test, we run the minimum amount of steps, and resolve the output with math and functions.
	logConfig := log.Config{
		Level:       log.LevelDebug,
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
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(justMathWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"a": int64(2),
		"b": int64(2),
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"result": "4 and Plugin slept for 0 ms.",
	})
}

func TestWorkflowWithEscapedCharacters(t *testing.T) {
	// For this test, we have escapable characters in the input and in the expressions
	// to make sure they are handled properly.
	logConfig := log.Config{
		Level:       log.LevelDebug,
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
		builtinfunctions.GetFunctions(),
	))
	fileData, err := os.ReadFile("./test_workflows/escaped_characters_workflow.yaml")
	assert.NoError(t, err)
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML(fileData))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"a": `\\\\`, // Start with four. There should still be four in the output.
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
	expectedString := `"\\\\" and "\\" and "'"'" and "	" \t Plugin slept for 0 ms.`
	expectedOutput := map[any]any{
		"result_raw_inlined":              expectedString,
		"result_raw_flow_scalar":          expectedString,
		"result_inlined_single_quote":     expectedString,
		"result_inlined_double_quote":     expectedString,
		"result_flow_scalar_single_quote": expectedString,
		"result_flow_scalar_double_quote": expectedString,
	}
	outputAsMap := outputData.(map[any]any)
	for expectedKey, expectedValue := range expectedOutput {
		value, outputHasExpectedKey := outputAsMap[expectedKey]
		if !outputHasExpectedKey {
			t.Errorf("output missing expected key %q", expectedKey)
		} else if value != expectedValue {
			t.Errorf("case %q failed; expected (%s), got (%s)", expectedKey, expectedValue, value)
		}

	}
}

var workflowWithOutputSchemaMalformed = `
version: v0.2.0
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
      wait_time_ms: 1
outputs:
  success:
    first_step_output: !expr $.steps.long_wait.outputs
outputSchema:
  success:
    schema:
      root: RootObjectOut
      objects: 
        RootObjectOut: 
          id: RootObjectOut
          properties: {}`

func TestWorkflow_Execute_Error_MalformedOutputSchema(t *testing.T) {
	pwf, err := createTestExecutableWorkflow(t, workflowWithOutputSchemaMalformed, map[string][]byte{})
	assert.NoError(t, err)
	_, _, err = pwf.Execute(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bug: output schema cannot unserialize")
}

var childNamespacedScopesWorkflow = `
version: v0.2.0
input:
  root: SubWorkflowInput
  objects:
    SubWorkflowInput:
      id: SubWorkflowInput
      properties:
        wait_input_property:
          type:
            type_id: ref
            id: wait-input
            namespace: $.steps.simple_wait.starting.inputs.input
steps:
  simple_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input: !expr $.input.wait_input_property
outputs:
  success:
    simple_wait_output: !expr $.steps.simple_wait.outputs.success
`

func TestWorkflowWithNamespacedScopes(t *testing.T) {
	// Run a workflow where the input uses a reference to one of the workflow's steps.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, childNamespacedScopesWorkflow),
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"wait_input_property": map[string]any{"wait_time_ms": 0},
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
}

var parentNestedNamespacedScopesWorkflow1 = `
version: v0.2.0
input:
  root: ParentWorkflow1Input
  objects:
    ParentWorkflow1Input:
      id: ParentWorkflow1Input
      properties:
        sub_workflow_input:
          type:
            type_id: list
            items:
              # References the input of the sub-workflow.
              type_id: ref
              id: SubWorkflowInput
              namespace: $.steps.sub_workflow_loop.execute.inputs.items
steps:
  sub_workflow_loop:
    kind: foreach
    items: !expr $.input.sub_workflow_input
    workflow: child_workflow_namespaced_scopes.yaml
    parallelism: 1
outputs:
  success:
    sub_workflow_result: !expr $.steps.sub_workflow_loop.outputs.success
`

var parentNestedNamespacedScopesWorkflow2 = `
version: v0.2.0
input:
  root: ParentWorkflow1Input
  objects:
    ParentWorkflow1Input:
      id: ParentWorkflow1Input
      properties:
        sub_workflow_input:
          type:
            type_id: list
            items:
              type_id: ref
              id: SubWorkflowInput
    SubWorkflowInput:
      # Re-constructs the object from the sub-workflow, but references the step object used.
      id: SubWorkflowInput
      properties:
        wait_input_property:
          type:
            type_id: ref
            id: wait-input
            namespace: $.steps.sub_workflow_loop.execute.inputs.items.wait_input_property
steps:
  sub_workflow_loop:
    kind: foreach
    items: !expr $.input.sub_workflow_input
    workflow: child_workflow_namespaced_scopes.yaml
    parallelism: 1
outputs:
  success:
    sub_workflow_result: !expr $.steps.sub_workflow_loop.outputs.success
`

func TestNestedWorkflowWithNamespacedScopes(t *testing.T) {
	// Combine namespaced scopes with foreach. Manually create the registry to allow this.
	logConfig := log.Config{
		Level:       log.LevelDebug,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	cfg := &config.Config{
		Log: logConfig,
	}
	factories := workflowFactory{
		config: cfg,
	}
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"builtin": map[string]any{
				"deployer_name": "test-impl",
				"deploy_time":   "0",
			},
		}),
	)
	stepRegistry, err := stepregistry.New(
		pluginProvider,
		lang.Must2(foreach.New(logger, factories.createYAMLParser, factories.createWorkflow)),
	)
	assert.NoError(t, err)

	factories.stepRegistry = stepRegistry
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	for workflowIndex, parentWorkflow := range []string{parentNestedNamespacedScopesWorkflow1, parentNestedNamespacedScopesWorkflow2} {
		t.Logf("running parent workflow %d", workflowIndex)
		wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(parentWorkflow)))
		preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{
			"child_workflow_namespaced_scopes.yaml": []byte(childNamespacedScopesWorkflow),
		}))
		subWorkflowInput := map[string]any{
			"wait_input_property": map[string]any{"wait_time_ms": 0},
		}
		outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
			"sub_workflow_input": []map[string]any{
				subWorkflowInput,
			},
		})
		assert.NoError(t, err)
		assert.Equals(t, outputID, "success")
	}
}

var inputCancelledStepWorkflow = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        step_cancelled:
          type:
            type_id: bool
steps:
  simple_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    deploy:
      deployer_name: "test-impl"
      # stop_if doesn't create any dependency, so we must keep the step in the deployment
      # stage long enough for the cancellation to occur at the intended stage.
      deploy_time: 25 # ms
    input:
      # The actual wait time should not matter for this test because the intention is to
      # to cancel it before it is run.
      wait_time_ms: 0
    stop_if: $.input.step_cancelled
outputs:
  did-not-cancel:
    simple_wait_output: !expr $.steps.simple_wait.outputs.success
`

func TestInputCancelledStepWorkflow(t *testing.T) {
	// Run a workflow where the step is cancelled before deployment.
	// The dependencies of `stop_if` are already resolved when the step
	// gets deployed. This causes a cancellation that is executed around
	// the time that the step begins processing.
	// This test configures the deployer with a delay to allow the
	// cancellation to be delivered before the deployment finishes.
	// In addition, the cancelled step is the only step, so its
	// cancellation must be handled properly to prevent a deadlock.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, inputCancelledStepWorkflow),
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"step_cancelled": true,
	})
	assert.Equals(t, outputID, "")
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
}

var inputDisabledStepWorkflow = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        step_enabled:
          type:
            type_id: bool
steps:
  simple_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 20
    enabled: !expr $.input.step_enabled
outputs:
  workflow-success:
    simple_wait_output: !expr $.steps.simple_wait.outputs.success
`

func TestInputDisabledStepWorkflow(t *testing.T) {
	// Run a workflow with one step that has its enablement state
	// set by the input. The output depends on successful output
	// of the step that can be disabled, so this test case also
	// tests that the failure caused when it's disabled doesn't
	// lead to a deadlock.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, inputDisabledStepWorkflow),
	)
	// The workflow should pass with it enabled
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"step_enabled": true,
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "workflow-success")
	// The workflow should fail with it disabled because the output cannot be resolved.
	_, _, err = preparedWorkflow.Execute(context.Background(), map[string]any{
		"step_enabled": false,
	})
	assert.Error(t, err)
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
}

var dynamicDisabledStepWorkflow = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        sleep_time:
          type:
            type_id: integer
steps:
  initial_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: !expr $.input.sleep_time # ms
  toggled_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    enabled: !expr $.steps.initial_wait.outputs.success.message == "Plugin slept for 5 ms."
outputs:
  success:
    initial_wait_output: !expr $.steps.initial_wait.outputs.success
    toggled_wait_output: !expr $.steps.toggled_wait.outputs.success
  disabled:
    initial_wait_output: !expr $.steps.initial_wait.outputs.success
    toggled_wait_output: !expr $.steps.toggled_wait.disabled.output
`

func TestDelayedDisabledStepWorkflow(t *testing.T) {
	// Run a workflow where the step is disabled by a value that isn't available
	// at the start of the workflow; in this case the step is disabled from
	// another step's output.
	// This workflow has an output for success and an output for disabled.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, dynamicDisabledStepWorkflow),
	)
	// The second step expects a 5ms sleep/wait.
	// Pass with a 5ms input.
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"sleep_time": 5,
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "success")
	// Fail with a non-5ms input.
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"sleep_time": 4,
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "disabled")
	assert.InstanceOf[map[any]any](t, outputData)
	outputMap := outputData.(map[any]any)
	assert.MapContainsKey(t, "toggled_wait_output", outputMap)
	toggledOutput := outputMap["toggled_wait_output"]
	assert.InstanceOf[map[any]any](t, toggledOutput)
	toggledOutputMap := toggledOutput.(map[any]any)
	assert.MapContainsKey(t, "message", toggledOutputMap)
	assert.Equals(t, toggledOutputMap["message"], "Step toggled_wait/wait disabled")
}

var testExpressionWithExtraWhitespace = `
version: v0.2.0
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
    leading-whitespace: !expr "    $.steps.wait_1.outputs.success.message"
    trailing-whitespace: !expr "$.steps.wait_1.outputs.success.message     "
    # Use | instead of |- to keep the newline at the end.
    trailing-newline: !expr |
      $.steps.wait_1.outputs.success.message
`

func TestExpressionWithWhitespace(t *testing.T) {
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, testExpressionWithExtraWhitespace),
	)
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"leading-whitespace":  "Plugin slept for 0 ms.",
		"trailing-whitespace": "Plugin slept for 0 ms.",
		"trailing-newline":    "Plugin slept for 0 ms.",
	})
}

var multiDependencyFailureWorkflowWithDisabling = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        fail_purposefully:
          type:
            type_id: bool
steps:
  disabled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    enabled: !expr '!$.input.fail_purposefully'
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 5000
outputs:
  workflow-success:
    simple_wait_output: !expr $.steps.disabled_step.outputs.success
    long_wait_output: !expr $.steps.long_wait.outputs.success
`

func TestMultiDependencyWorkflowFailureWithDisabling(t *testing.T) {
	// Tests failure when one dependency is disabled immediately, and the other one completes later.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowWithDisabling),
	)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	_, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"fail_purposefully": true,
	})
	duration := time.Since(startTime)
	assert.Error(t, err)
	t.Logf("MultiDependencyFailureWithDisabling workflow failed purposefully in %d ms", duration.Milliseconds())
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
	assert.LessThan(t, duration.Milliseconds(), 500) // It will take 5 seconds if it fails to fail early.
}

var multiDependencyFailureWorkflowWithCancellation = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        fail_purposefully:
          type:
            type_id: bool
steps:
  canceled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    stop_if: !expr '$.input.fail_purposefully'
    deploy:
      deployer_name: "test-impl"
      deploy_time: 10 # 10 ms of delay to make the cancellation more reliable.
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 5000
outputs:
  workflow-success:
    cancelled_step_output: !expr $.steps.canceled_step.outputs.success
    long_wait_output: !expr $.steps.long_wait.outputs.success
`

func TestMultiDependencyWorkflowFailureWithCancellation(t *testing.T) {
	// Tests failure when one dependency is cancelled immediately, and the other one completes later.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowWithCancellation),
	)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	_, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"fail_purposefully": true,
	})
	duration := time.Since(startTime)
	assert.Error(t, err)
	t.Logf("MultiDependencyFailureWithCancellation workflow failed purposefully in %d ms", duration.Milliseconds())
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
	assert.LessThan(t, duration.Seconds(), 4) // It will take 5 seconds if it fails to fail early.
}

var multiDependencyFailureWorkflowWithErrorOut = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        fail_purposefully:
          type:
            type_id: bool
steps:
  failed_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: hello
    input:
      fail: !expr $.input.fail_purposefully
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 7000
outputs:
  workflow-success:
    failed_output: !expr $.steps.failed_step.outputs.success
    long_wait_output: !expr $.steps.long_wait.outputs.success
`

func TestMultiDependencyWorkflowFailureWithErrorOut(t *testing.T) {
	// Tests failure when one dependency fails immediately due to the wrong output, and the
	// other one completes later.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowWithErrorOut),
	)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	_, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"fail_purposefully": true,
	})
	duration := time.Since(startTime)
	assert.Error(t, err)
	t.Logf("MultiDependencyFailureWithErrorOut workflow failed purposefully in %d ms", duration.Milliseconds())
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
	// If it takes 5 seconds, then there was a deadlock in the client.
	// If it takes 6 seconds, then it waited for the second step.
	assert.LessThan(t, duration.Milliseconds(), 5500)
}

var multiDependencyFailureWorkflowWithDeployFailure = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties:
        fail_purposefully:
          type:
            type_id: bool
steps:
  failed_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    deploy:
      deployer_name: "test-impl"
      deploy_succeed: !expr '!$.input.fail_purposefully'
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 7000
outputs:
  workflow-success:
    failed_step_output: !expr $.steps.failed_step.outputs
    simple_wait_output: !expr $.steps.long_wait.outputs
`

func TestMultiDependencyWorkflowFailureWithDeployFailure(t *testing.T) {
	// Tests failure when one dependency fails (due to failed deployment) immediately,
	// and the other one fails later.
	// In this specific test the output depends on the `steps.failed_step.outputs` node
	// instead of the `steps.failed_step.outputs.success` node because they are handled
	// differently in the engine.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowWithDeployFailure),
	)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	_, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"fail_purposefully": true,
	})
	duration := time.Since(startTime)
	assert.Error(t, err)
	t.Logf("MultiDependency workflow failed purposefully in %d ms", duration.Milliseconds())
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
	// If it takes 5 seconds, then there was a deadlock in the client.
	// If it takes 6 seconds, then it waited for the second step.
	assert.LessThan(t, duration.Milliseconds(), 5500)
}

var multiDependencyFailureWorkflowWithDoubleFailure = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties: {}
steps:
  failed_step_A:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 0
    deploy: # This fails first.
      deployer_name: "test-impl"
      deploy_time: 0
      deploy_succeed: !expr 'false'
  quick_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 1 # Second, this succeeds, which cancels the second failing step.
  failed_step_B:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    wait_for: !expr $.steps.failed_step_A.outputs # Makes it unresolvable
    stop_if: !expr $.steps.quick_step.outputs # Hopefully triggers the second resolution.
    input:
      wait_time_ms: 0
    deploy:
      deployer_name: "test-impl"
      deploy_succeed: !expr 'true'
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 10
outputs:
  workflow-success:
    failed_step_output: !expr $.steps.failed_step_A.outputs
  wait-only: # For the error to be exposed, we need an alternative output that persists beyond the error.
    wait_output: !expr $.steps.long_wait.outputs
`

func TestMultiDependencyWorkflowFailureDoubleFailure(t *testing.T) {
	// Creates a scenario where step B's starting (due to wait-for) depends on step A's outputs,
	// making A's outputs become unresolvable, while at the same time the step that needs that info (B) crashes.
	// Transitioning B to crashed resolves starting, so that is in conflict with the unresolvable
	// state propagated from step A.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowWithDoubleFailure),
	)

	startTime := time.Now() // Right before execution to not include pre-processing time.
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	assert.NoError(t, err)
	assert.Equals(t, outputID, "wait-only")
	t.Logf("MultiDependency DoubleFailure finished in %d ms", duration.Milliseconds())
}

var multiDependencyFailureWorkflowContextCancelled = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties: {}
steps:
  wait_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 500
  cancelled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    stop_if: !expr $.steps.wait_step.outputs # The context will get cancelled before this triggers.
    input:
      wait_time_ms: 0
    deploy:
      deployer_name: "test-impl"
      deploy_succeed: !expr 'true'
      deploy_time: 5 # ms
outputs:
  finished:
    cancelled_step_output: !expr $.steps.cancelled_step.outputs
  wait-only: # The workflow needs to keep running after the cancelled step exits.
    wait_output: !expr $.steps.wait_step.outputs
`

func TestMultiDependencyWorkflowContextCanceled(t *testing.T) {
	// A scenario where the step's inputs are resolvable, but the context is cancelled, resulting
	// in a possible conflict with the cancelled step stage DAG node.
	// To do this, create a multi-dependency setup, finish the step, then cancel the workflow
	// before the workflow finishes.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyFailureWorkflowContextCancelled),
	)

	ctx, timeout := context.WithTimeout(context.Background(), time.Millisecond*30)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	_, _, err := preparedWorkflow.Execute(ctx, map[string]any{})
	duration := time.Since(startTime)
	assert.NoError(t, err)
	timeout()
	t.Logf("MultiDependency ContextCanceled finished in %d ms", duration.Milliseconds())
}

var multiDependencyDependOnClosedStepPostDeployment = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties: {}
steps:
  wait_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 1
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 5000
  cancelled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    stop_if: !expr $.steps.wait_step.outputs
    wait_for: !expr $.steps.long_wait.outputs
    input:
      wait_time_ms: 0
    # The deploy section is blank, so it will target a post-deployment cancellation.
outputs:
  finished:
    cancelled_step_output: !expr $.steps.cancelled_step.outputs
  closed: # The workflow needs to keep running after the cancelled step exits.
    closed_output: !expr $.steps.cancelled_step.closed.result
  wait_finished:
    wait_output: !expr $.steps.long_wait.outputs
`

func TestMultiDependencyDependOnClosedStepPostDeployment(t *testing.T) {
	// This has the output depend on the closed output of a step.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyDependOnClosedStepPostDeployment),
	)

	startTime := time.Now() // Right before execution to not include pre-processing time.
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	t.Logf("MultiDependency DependOnClosedStepPostDeployment finished in %d ms", duration.Milliseconds())
	assert.NoError(t, err)
	assert.Equals(t, outputID, "closed")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"closed_output": map[any]any{
			"cancelled":       true,
			"close_requested": false,
		},
	})
}

var multiDependencyDependOnClosedDeployment = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties: {}
steps:
  wait_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 1
  cancelled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    stop_if: !expr $.steps.wait_step.outputs
    input:
      wait_time_ms: 500
    # The deploy section has a delay black, so it will target a mid-deployment cancellation.
    closure_wait_timeout: 0
    deploy:
      deployer_name: "test-impl"
      deploy_time: 10 # ms. If this is too low, there will be a race that results in run fail instead of closure.
outputs:
  finished:
    cancelled_step_output: !expr $.steps.cancelled_step.outputs
  closed: # The workflow needs to keep running after the cancelled step exits.
    closed_output: !expr $.steps.cancelled_step.closed.result
`

func TestMultiDependencyDependOnClosedStepDeployment(t *testing.T) {
	// This has the output depend on the closed output of a step.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyDependOnClosedDeployment),
	)

	startTime := time.Now() // Right before execution to not include pre-processing time.
	outputID, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	t.Logf("MultiDependency DependOnClosedStep finished in %d ms", duration.Milliseconds())
	assert.NoError(t, err)
	assert.Equals(t, outputID, "closed")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"closed_output": map[any]any{
			"cancelled":       true,
			"close_requested": false,
		},
	})
}

var multiDependencyDependOnContextDoneDeployment = `
version: v0.2.0
input:
  root: WorkflowInput
  objects:
    WorkflowInput:
      id: WorkflowInput
      properties: {}
steps:
  wait_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    closure_wait_timeout: 0
    input:
      wait_time_ms: 1000
  not_enabled_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    enabled: !expr $.steps.wait_step.outputs.success.message == "Plugin slept for 100 ms."
    input:
      wait_time_ms: 0
outputs:
  finished:
    cancelled_step_output: !expr $.steps.not_enabled_step.outputs
  closed: # The workflow needs to keep running after the cancelled step exits.
    closed_output: !expr $.steps.not_enabled_step.closed.result
`

func TestMultiDependencyDependOnContextDoneDeployment(t *testing.T) {
	// A scenario where you close the context but still expect an output by depending on the closed output.
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, multiDependencyDependOnContextDoneDeployment),
	)

	ctx, timeout := context.WithTimeout(context.Background(), time.Millisecond*10)
	startTime := time.Now() // Right before execution to not include pre-processing time.
	outputID, outputData, err := preparedWorkflow.Execute(ctx, map[string]any{})
	duration := time.Since(startTime)
	assert.NoError(t, err)
	timeout()
	assert.Equals(t, outputID, "closed")
	assert.Equals(t, outputData.(map[any]any), map[any]any{
		"closed_output": map[any]any{
			"cancelled":       false,
			"close_requested": true,
		},
	})
	t.Logf("MultiDependency DependOnClosedStep finished in %d ms", duration.Milliseconds())
}

var multiDependencyForEachParent = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  subworkflow:
    kind: foreach
    items:
    - {}
    workflow: subworkflow.yaml
  long_wait:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: wait
    input:
      wait_time_ms: 5000
outputs:
  success:
    long_wait_output: !expr $.steps.long_wait.outputs
    subworkflow_output: !expr $.steps.subworkflow.outputs.success
`

var multiDependencyForEachSubwf = `
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties: {}
steps:
  failed_step:
    plugin:
      src: "n/a"
      deployment_type: "builtin"
    step: hello
    input:
      fail: !expr true
outputs:
  success:
    b: !expr $.steps.failed_step.outputs.success
`

func TestMultiDependencyForeach(t *testing.T) {
	// This test runs a workflow with a wait and a subworkfow.
	// This tests to ensure that the parent workflow immediately detects
	// and acts on the missing dependency caused by the error output
	// coming from the subworkflow.

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
	factories := workflowFactory{
		config: cfg,
	}
	deployerRegistry := deployerregistry.New(
		deployer.Any(testimpl.NewFactory()),
	)

	pluginProvider := assert.NoErrorR[step.Provider](t)(
		plugin.New(logger, deployerRegistry, map[string]interface{}{
			"builtin": map[string]any{
				"deployer_name": "test-impl",
				"deploy_time":   "0",
			},
		}),
	)
	stepRegistry, err := stepregistry.New(
		pluginProvider,
		lang.Must2(foreach.New(logger, factories.createYAMLParser, factories.createWorkflow)),
	)
	assert.NoError(t, err)

	factories.stepRegistry = stepRegistry
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(multiDependencyForEachParent)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{
		"subworkflow.yaml": []byte(multiDependencyForEachSubwf),
	}))
	startTime := time.Now() // Right before execute to not include pre-processing time.
	_, _, err = preparedWorkflow.Execute(context.Background(), map[string]any{})
	duration := time.Since(startTime)
	assert.Error(t, err)

	t.Logf("MultiDependency workflow failed purposefully in %d ms", duration.Milliseconds())
	var typedError *workflow.ErrNoMorePossibleOutputs
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T (%s)", err, err)
	}
	assert.LessThan(t, duration.Milliseconds(), 400)
}

func createTestExecutableWorkflow(t *testing.T, workflowStr string, workflowCtx map[string][]byte) (workflow.ExecutableWorkflow, error) {
	logConfig := log.Config{
		Level:       log.LevelDebug,
		Destination: log.DestinationStdout,
	}
	logger := log.New(logConfig)
	cfg := &config.Config{Log: logConfig}
	stepRegistry := NewTestImplStepRegistry(logger, t)
	executor := lang.Must2(workflow.NewExecutor(logger, cfg, stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(workflowStr)))
	return executor.Prepare(wf, workflowCtx)
}

const printNamespaceResponseOutput = `OBJECT                   NAMESPACE   
ClosedInfo                $.steps.long_wait.closed.outputs.result
Crashed                   $.steps.long_wait.crashed.outputs.error
DeployError               $.steps.long_wait.deploy_failed.outputs.error
DisabledMessageOutput     $.steps.long_wait.disabled.outputs.output
EnabledOutput             $.steps.long_wait.enabling.outputs.resolved
output                    $.steps.long_wait.outputs.outputs.cancelled_early
output                    $.steps.long_wait.outputs.outputs.success
output                    $.steps.long_wait.outputs.outputs.terminated_early
wait-input                $.steps.long_wait.starting.inputs.input
StartedOutput             $.steps.long_wait.starting.outputs.started
`

func TestPrintObjectNamespaceTable(t *testing.T) {
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, fourSecWaitWorkflowDefinition),
	)
	buf := bytes.NewBuffer(nil)
	workflow.PrintObjectNamespaceTable(buf, preparedWorkflow.Namespaces(), nil)
	assert.Equals(t, buf.String(), printNamespaceResponseOutput)
}

func TestPrintObjectNamespaceTable_EmptyNamespace(t *testing.T) {
	logger := log.NewLogger(log.LevelDebug, log.NewTestWriter(t))
	buf := bytes.NewBuffer(nil)
	workflow.PrintObjectNamespaceTable(buf, map[string]map[string]*schema.ObjectSchema{}, logger)
	assert.Equals(t, buf.String(), ``)
}

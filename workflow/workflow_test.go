package workflow_test

import (
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
	var typedError *workflow.ErrNoMorePossibleSteps
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T", err)
	}
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
      deployer_name: "test-impl"
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

func TestWithDoubleSerializationDetection(t *testing.T) {
	// Just a single wait
	preparedWorkflow := assert.NoErrorR[workflow.ExecutableWorkflow](t)(
		getTestImplPreparedWorkflow(t, simpleValidLiteralInputWaitWorkflowDefinition),
	)
	// First, get the root object
	inputSchema := preparedWorkflow.Input()
	rootObject := inputSchema.Objects()[inputSchema.Root()]
	errorDetect := util.NewInvalidSerializationDetectorSchema()
	// Inject the error detector into the object
	rootObject.PropertiesValue["error_detector"] = schema.NewPropertySchema(
		errorDetect,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	outputID, _, err := preparedWorkflow.Execute(context.Background(), map[string]any{
		"error_detector": "original input",
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	// Confirm that, while we did no double-unserializations or double-serializations,
	// we did do at least one single one.
	assert.Equals(t, errorDetect.SerializeCnt+errorDetect.UnserializeCnt > 0, true)

	// again, but with a default value for the error detector
	errorDetect = util.NewInvalidSerializationDetectorSchema()
	rootObject.PropertiesValue["error_detector"] = schema.NewPropertySchema(
		errorDetect,
		nil,
		true,
		nil,
		nil,
		nil,
		schema.PointerTo[string]("default"),
		nil,
	)
	outputID, _, err = preparedWorkflow.Execute(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	assert.Equals(t, errorDetect.SerializeCnt+errorDetect.UnserializeCnt > 0, true)

	// again, but override error detector default with input
	errorDetect = util.NewInvalidSerializationDetectorSchema()
	rootObject.PropertiesValue["error_detector"] = schema.NewPropertySchema(
		errorDetect,
		nil,
		true,
		nil,
		nil,
		nil,
		schema.PointerTo[string]("default"),
		nil,
	)
	outputID, _, err = preparedWorkflow.Execute(context.Background(), map[string]any{
		"error_detector": "original input",
	})
	assert.NoError(t, err)
	assert.Equals(t, outputID, "a")
	assert.Equals(t, errorDetect.SerializeCnt+errorDetect.UnserializeCnt > 0, true)
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

var fiveSecWaitWorkflowDefinition = `
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
      wait_time_ms: 5000
outputs:
  success:
    first_step_output: !expr $.steps.long_wait.outputs
`

func TestEarlyContextCancellation(t *testing.T) {
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
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(fiveSecWaitWorkflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	// Cancel the context after 3 ms to simulate cancellation with ctrl-c.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	startTime := time.Now() // Right before execute to not include pre-processing time.
	//nolint:dogsled
	_, _, _ = preparedWorkflow.Execute(ctx, map[string]any{})
	cancel()

	duration := time.Since(startTime)
	t.Logf("Test execution time: %s", duration)
	if duration >= 1000*time.Millisecond {
		t.Fatalf("Test execution time is greater than 1000 milliseconds; Is the workflow properly cancelling?")
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

package plugin_test

import (
	"fmt"
	"go.arcalot.io/assert"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	deployer_registry "go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/plugin"
	testdeployer "go.flow.arcalot.io/testdeployer"
	stubdeployer "go.flow.arcalot.io/testdeployer/stub"
	"sync"
	"testing"
)

type deployFailStageChangeHandler struct {
	message chan string
}

func (s *deployFailStageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool, _ *sync.WaitGroup) {

}

func (s *deployFailStageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
	_ *sync.WaitGroup,
) {
	if previousStage != string(plugin.StageIDDeployFailed) {
		panic(fmt.Errorf("invalid previous stage: %s", previousStage))
	}
	if previousStageOutputID == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}
	if *previousStageOutputID != "error" {
		panic(fmt.Errorf("invalid previous stage output ID: %s", *previousStageOutputID))
	}
	if previousStageOutput == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}
	message := (*previousStageOutput).(plugin.DeployFailed).Error

	s.message <- message
}

func (s *deployFailStageChangeHandler) OnStepStageFailure(_ step.RunningStep, _ string, _ *sync.WaitGroup, _ error) {

}

type startFailStageChangeHandler struct {
	message chan string
}

func (s *startFailStageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool, _ *sync.WaitGroup) {

}

func (s *startFailStageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
	_ *sync.WaitGroup,
) {
	if previousStage != string(plugin.StageIDCrashed) {
		panic(fmt.Errorf("invalid previous stage: %s", previousStage))
	}
	if previousStageOutputID == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}
	if *previousStageOutputID != "error" {
		panic(fmt.Errorf("invalid previous stage output ID: %s", *previousStageOutputID))
	}
	if previousStageOutput == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}

	message := (*previousStageOutput).(plugin.Crashed).Output

	s.message <- message
}

func (s *startFailStageChangeHandler) OnStepStageFailure(_ step.RunningStep, _ string, _ *sync.WaitGroup, _ error) {

}

type stageChangeHandler struct {
	message chan string
}

func (s *stageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool, _ *sync.WaitGroup) {

}

func (s *stageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
	_ *sync.WaitGroup,
) {
	if previousStage != string(plugin.StageIDOutput) {
		panic(fmt.Errorf("invalid previous stage: %s", previousStage))
	}
	if previousStageOutputID == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}
	if *previousStageOutputID != "success" {
		panic(fmt.Errorf("invalid previous stage output ID: %s", *previousStageOutputID))
	}
	if previousStageOutput == nil {
		panic(fmt.Errorf("no previous stage output ID"))
	}
	message := (*previousStageOutput).(map[any]any)["message"].(string)

	s.message <- message
}

func (s *stageChangeHandler) OnStepStageFailure(_ step.RunningStep, _ string, _ *sync.WaitGroup, _ error) {

}

func TestProvider_MissingDeployer(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	deployerRegistry := deployer_registry.New() // Empty. So it will error out.
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{
			"deployer_name": "test-impl",
		},
	}

	_, err := plugin.New(logger, deployerRegistry, workflowDeployerCfg)
	assert.Error(t, err)
}

func TestProvider_Utility(t *testing.T) {
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{"deployer_name": "test-impl"},
	}

	plp, err := plugin.New(log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	), deployer_registry.New(
		deployer.Any(testdeployer.NewFactory())), workflowDeployerCfg)
	assert.NoError(t, err)
	assert.Equals(t, plp.Kind(), "plugin")
	assert.NotNil(t, plp.ProviderSchema())
	assert.NotNil(t, plp.RunProperties())
	assert.NotNil(t, plp.Lifecycle())

	stepSchema := map[string]any{
		"plugin": map[string]any{
			"src":             "simulation",
			"deployment_type": "builtin",
		},
	}
	byteSchema := map[string][]byte{}

	runnable, err := plp.LoadSchema(stepSchema, byteSchema)
	assert.NoError(t, err)

	assert.NotNil(t, runnable.RunSchema())

	_, err = runnable.Lifecycle(map[string]any{"step": "wait"})
	assert.NoError(t, err)

	_, err = runnable.Lifecycle(map[string]any{"step": "hello"})
	assert.NoError(t, err)

	// There is more than one step, so no specified one will cause an error.
	_, err = runnable.Lifecycle(map[string]any{"step": nil})
	assert.Error(t, err)
}

func TestProvider_HappyError(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{
			"deployer_name": "test-impl"},
		"stubby": map[string]any{
			"deployer_name": "test-stub",
		},
	}

	deployerRegistry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()),
		deployer.Any(stubdeployer.NewFactory()))

	// Deployer type not in deployer registry
	_, err := plugin.New(logger, deployerRegistry, map[string]any{
		"wrong": map[string]any{
			"deployer_name": "test-impl",
		}})
	assert.Error(t, err)

	// Deployment type stubby is in deployer registry, but does not
	// match the deployment type for test-impl
	_, err = plugin.New(logger, deployerRegistry, map[string]any{
		"stubby": map[string]any{
			"deployer_name": "test-impl",
		}})
	assert.Error(t, err)

	plp, err := plugin.New(logger, deployerRegistry, workflowDeployerCfg)
	assert.NoError(t, err)

	stepSchema := map[string]any{
		"plugin": map[string]any{
			"src":             "simulation",
			"deployment_type": "builtin"},
	}
	byteSchema := map[string][]byte{}

	runnable, err := plp.LoadSchema(stepSchema, byteSchema)
	assert.NoError(t, err)

	handler := &stageChangeHandler{
		message: make(chan string),
	}

	// start with a step id that is not in the schema
	_, err = runnable.Start(map[string]any{"step": "wrong_stepid"}, t.Name(), handler)
	assert.Error(t, err)

	// wait step
	running, err := runnable.Start(map[string]any{"step": "wait"}, t.Name(), handler)
	assert.NoError(t, err)

	// non-existent stage
	assert.Error(t, running.ProvideStageInput(
		"", nil))

	// deployer name and deployment type mismatch
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"deployer_name": "test-stub",
			"deploy_time":   1}},
	))

	// unserialize malformed deploy schema
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"deployer_name": "test-impl",
			"deploy_time":   "abc"}},
	))

	// Enable the step
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDEnabling),
		map[string]any{},
	))

	// Start with valid input
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"deployer_name": "test-impl",
			"deploy_time":   1}},
	))

	// provide deploy input a 2nd time
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"deployer_name": "test-impl",
			"deploy_time":   nil}},
	))

	// unserialize nil input schema error
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": nil},
	))

	// unserialize malformed input schema
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		// The schema must be for the object with the field wait_time_ms, which is an integer.
		map[string]any{"input": "not a valid value"},
	))

	waitTimeMs := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		// It is also valid to inline waitTimeMs.
		map[string]any{"input": map[string]any{"wait_time_ms": waitTimeMs}},
	))

	// provide running input a 2nd time
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{"wait_time_ms": waitTimeMs}},
	))

	message := <-handler.message
	assert.Equals(t,
		message,
		fmt.Sprintf("Plugin slept for %d ms.", waitTimeMs))

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDOutput))

	t.Cleanup(func() {
		assert.NoError(t, running.Close())
	})
}

func TestProvider_VerifyCancelSignal(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{
			"deployer_name": "test-impl",
		},
	}

	deployerRegistry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))

	plp, err := plugin.New(logger, deployerRegistry, workflowDeployerCfg)
	assert.NoError(t, err)

	runnable, err := plp.LoadSchema(
		map[string]any{
			"plugin": map[string]any{
				"src":             "simulation",
				"deployment_type": "builtin",
			}},
		map[string][]byte{})
	assert.NoError(t, err)
	assert.NotNil(t, runnable)

	waitLifecycle, err := runnable.Lifecycle(map[string]any{"step": "wait"})
	assert.NoError(t, err)
	// Verify that the expected lifecycle stage is there, then verify that cancel is disabled.
	waitCancelledStageIDIndex := assert.SliceContainsExtractor(t,
		func(schema step.LifecycleStageWithSchema) string {
			return schema.ID
		}, string(plugin.StageIDCancelled), waitLifecycle.Stages)
	waitStageIDCancelled := waitLifecycle.Stages[waitCancelledStageIDIndex]
	waitStopIfSchema := assert.MapContainsKey(t, "stop_if", waitStageIDCancelled.InputSchema)
	if waitStopIfSchema.Disabled {
		t.Fatalf("step wait's stop_if schema is disabled when the cancel signal is present.")
	}

	helloLifecycle, err := runnable.Lifecycle(map[string]any{"step": "hello"})
	assert.NoError(t, err)
	// Verify that the expected lifecycle stage is there, then verify that cancel is disabled.
	helloCancelledStageIDIndex := assert.SliceContainsExtractor(t,
		func(schema step.LifecycleStageWithSchema) string {
			return schema.ID
		}, string(plugin.StageIDCancelled), helloLifecycle.Stages)
	helloStageIDCancelled := helloLifecycle.Stages[helloCancelledStageIDIndex]
	helloStopIfSchema := assert.MapContainsKey(t, "stop_if", helloStageIDCancelled.InputSchema)
	if !helloStopIfSchema.Disabled {
		t.Fatalf("step hello's stop_if schema is not disabled when the cancel signal is not present.")
	}
}

func TestProvider_DeployFail(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	deployerRegistry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))
	deployTimeMs := 20
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{
			"deployer_name":  "test-impl",
			"deploy_time":    deployTimeMs,
			"deploy_succeed": true,
		},
	}

	plp, err := plugin.New(logger, deployerRegistry, workflowDeployerCfg)
	assert.NoError(t, err)

	stepSchema := map[string]any{
		"plugin": map[string]any{
			"src":             "simulation",
			"deployment_type": "builtin",
		},
	}
	byteSchema := map[string][]byte{}

	runnable, err := plp.LoadSchema(stepSchema, byteSchema)
	assert.NoError(t, err)

	handler := &deployFailStageChangeHandler{
		message: make(chan string),
	}

	// wait step
	running, err := runnable.Start(map[string]any{"step": "wait"}, t.Name(), handler)
	assert.NoError(t, err)

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{
			"deploy": map[string]any{
				"deployer_name":  "test-impl",
				"deploy_succeed": false,
				"deploy_time":    deployTimeMs,
			},
		},
	))

	waitTimeMs := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{"wait_time_ms": waitTimeMs}},
	))

	message := <-handler.message
	assert.Equals(t,
		message,
		fmt.Sprintf("intentional deployment fail after %d ms", deployTimeMs))

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDDeployFailed))

	t.Cleanup(func() {
		assert.NoError(t, running.Close())
	})
}

func TestProvider_StartFail(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	deployTimeMs := 20
	workflowDeployerCfg := map[string]any{
		"builtin": map[string]any{
			"deployer_name":  "test-impl",
			"deploy_time":    deployTimeMs,
			"deploy_succeed": true,
		},
	}

	plp, err := plugin.New(logger, deployer_registry.New(
		deployer.Any(testdeployer.NewFactory())), workflowDeployerCfg)
	assert.NoError(t, err)

	stepSchema := map[string]any{
		"plugin": map[string]any{
			"src":             "simulation",
			"deployment_type": "builtin"},
	}
	byteSchema := map[string][]byte{}

	runnable, err := plp.LoadSchema(stepSchema, byteSchema)
	assert.NoError(t, err)

	handler := &startFailStageChangeHandler{
		message: make(chan string),
	}

	running, err := runnable.Start(map[string]any{"step": "wait"}, t.Name(), handler)
	assert.NoError(t, err)

	// tell deployer that this run should not succeed
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"deployer_name":         "test-impl",
			"deploy_succeed":        true,
			"deploy_time":           deployTimeMs,
			"disable_plugin_writes": true}},
	))

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{
			"wait_time_ms": 50}},
	))

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDEnabling),
		map[string]any{},
	))

	// wait for message, but we don't care about its value
	<-handler.message

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDCrashed))

	t.Cleanup(func() {
		assert.NoError(t, running.Close())
	})
}

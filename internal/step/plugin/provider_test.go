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
	"testing"
)

type stageChangeHandler struct {
	message chan string
}

func (s *stageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool) {

}

func (s *stageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
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
	//message := (*previousStageOutput).(map[string]any)["message"].(string)
	message := (*previousStageOutput).(map[any]any)["message"].(string)

	s.message <- message
}

//func TestProvider_DeployerDocker(t *testing.T) {
//	// Depends on availability of local Docker service
//	logConfig := log.Config{
//		Level:       log.LevelError,
//		Destination: log.DestinationStdout,
//	}
//	logger := log.New(
//		logConfig,
//	)
//	workflow_deployer_cfg := map[string]any{
//		"type": "docker",
//	}
//
//	d_registry := deployer_registry.New(
//		deployer.Any(docker.NewFactory()))
//	plp, err := plugin.New(
//		logger,
//		d_registry,
//		workflow_deployer_cfg,
//	)
//	assert.NoError(t, err)
//	assert.Equals(t, plp.Kind(), "plugin")
//
//	step_schema := map[string]any{
//		"plugin": "ghcr.io/janosdebugs/arcaflow-example-plugin",
//	}
//	byte_schema := map[string][]byte{}
//
//	runnable, err := plp.LoadSchema(step_schema, byte_schema)
//	assert.NoError(t, err)
//
//	handler := &stageChangeHandler{
//		message: make(chan string),
//	}
//
//	running, err := runnable.Start(map[string]any{}, handler)
//	assert.NoError(t, err)
//
//	assert.NoError(t, running.ProvideStageInput(
//		string(plugin.StageIDDeploy),
//		map[string]any{"deploy": nil},
//	))
//
//	assert.NoError(t, running.ProvideStageInput(
//		string(plugin.StageIDRunning),
//		map[string]any{"input": map[string]any{"name": "Arca Lot"}},
//	))
//
//	message := <-handler.message
//	assert.Equals(t, message, "Hello, Arca Lot!")
//}

func TestProvider_Happy(t *testing.T) {
	logConfig := log.Config{
		Level:       log.LevelError,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	workflow_deployer_cfg := map[string]any{
		"type": "test-impl",
	}

	d_registry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))
	plp, err := plugin.New(
		logger,
		d_registry,
		workflow_deployer_cfg,
	)
	assert.NoError(t, err)
	assert.Equals(t, plp.Kind(), "plugin")

	assert.NotNil(t, plp.ProviderSchema())
	assert.NotNil(t, plp.RunProperties())
	assert.NotNil(t, plp.Lifecycle())

	step_schema := map[string]any{
		"plugin": "simulation",
	}
	byte_schema := map[string][]byte{}

	runnable, err := plp.LoadSchema(step_schema, byte_schema)
	assert.NoError(t, err)

	assert.NotNil(t, runnable.RunSchema())

	_, err = runnable.Lifecycle(map[string]any{"step": "wait"})
	assert.NoError(t, err)

	_, err = runnable.Lifecycle(map[string]any{"step": nil})
	assert.NoError(t, err)

	handler := &stageChangeHandler{
		message: make(chan string),
	}

	running, err := runnable.Start(map[string]any{}, handler)
	assert.NoError(t, err)

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": nil},
	))

	wait_time_ms := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDRunning),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	message := <-handler.message
	msg_expected := fmt.Sprintf("Plugin slept for %d ms.", wait_time_ms)
	assert.Equals(t, message, msg_expected)

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDOutput))
	stages_happy := []string{
		string(plugin.StageIDDeploy),
		string(plugin.StageIDRunning),
		string(plugin.StageIDOutput)}
	stgs := running.Stages()
	assert.Equals(t, stgs.Values(), stages_happy)
}

func TestProvider_HappyError(t *testing.T) {
	logConfig := log.Config{
		Level:       log.LevelError,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)
	workflow_deployer_cfg := map[string]any{
		"type": "test-impl",
	}

	d_registry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))

	plp, err := plugin.New(
		logger,
		d_registry,
		map[string]any{"deployer_cfg": "bad"},
	)
	assert.Error(t, err)

	plp, err = plugin.New(
		logger,
		d_registry,
		workflow_deployer_cfg,
	)
	assert.NoError(t, err)

	runnable, err := plp.LoadSchema(
		map[string]any{"plugin": "simulation"}, map[string][]byte{})
	assert.NoError(t, err)

	handler := &stageChangeHandler{
		message: make(chan string),
	}

	// start with a step id that is not in the schema
	_, err = runnable.Start(map[string]any{"step": "wrong_stepid"}, handler)
	assert.Error(t, err)

	// default step id
	running, err := runnable.Start(map[string]any{"step": nil}, handler)
	assert.NoError(t, err)

	// non-existent stage
	assert.Error(t, running.ProvideStageInput(
		"", nil))

	// unserialize malformed deploy schema
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		//map[string]any{"deploy": nil},
		map[string]any{"deploy": map[string]any{
			"type":        "test-impl",
			"deploy_time": "abc"}},
	))

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		//map[string]any{"deploy": nil},
		map[string]any{"deploy": map[string]any{
			"type":        "test-impl",
			"deploy_time": 1}},
	))

	// provide deploy input a 2nd time
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		//map[string]any{"deploy": nil},
		map[string]any{"deploy": map[string]any{
			"type":        "test-impl",
			"deploy_time": 1}},
	))

	// unserialize nil input schema error
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDRunning),
		map[string]any{"input": nil},
	))

	// unserialize malformed input schema
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDRunning),
		map[string]any{"input": 1},
	))

	wait_time_ms := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDRunning),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	// provide running input a 2nd time
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDRunning),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	message := <-handler.message
	msg_expected := fmt.Sprintf("Plugin slept for %d ms.", wait_time_ms)
	assert.Equals(t, message, msg_expected)

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDOutput))
	stages_happy := []string{
		string(plugin.StageIDDeploy),
		string(plugin.StageIDRunning),
		string(plugin.StageIDOutput)}

	stgs := running.Stages()
	assert.Equals(t, stgs.Values(), stages_happy)
}

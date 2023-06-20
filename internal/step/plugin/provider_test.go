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

type deployFailStageChangeHandler struct {
	message chan string
}

func (s *deployFailStageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool) {

}

func (s *deployFailStageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
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
	//message := (*previousStageOutput).(map[string]any)["message"].(string)
	message := (*previousStageOutput).(plugin.DeployFailed).Error

	s.message <- message
}

type runFailStageChangeHandler struct {
	message chan string
}

func (s *runFailStageChangeHandler) OnStageChange(_ step.RunningStep, _ *string, _ *string, _ *any, _ string, _ bool) {

}

func (s *runFailStageChangeHandler) OnStepComplete(
	_ step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
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

func TestProvider_Utility(t *testing.T) {
	workflow_deployer_cfg := map[string]any{
		"type": "test-impl",
	}

	plp, err := plugin.New(
		log.New(
			log.Config{
				Level:       log.LevelError,
				Destination: log.DestinationStdout,
			},
		),
		deployer_registry.New(
			deployer.Any(testdeployer.NewFactory())),
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
}

func TestProvider_HappyError(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
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
			"deploy_time": nil}},
	))

	// unserialize nil input schema error
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": nil},
	))

	// unserialize malformed input schema
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": 1},
	))

	wait_time_ms := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	// provide running input a 2nd time
	assert.Error(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	message := <-handler.message
	msg_expected := fmt.Sprintf("Plugin slept for %d ms.", wait_time_ms)
	assert.Equals(t, message, msg_expected)

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDOutput))
	//stages_happy := []string{
	//	string(plugin.StageIDDeploy),
	//	string(plugin.StageIDStarting),
	//	string(plugin.StageIDRunning),
	//	string(plugin.StageIDOutput)}
	//
	//stgs := running.Stages()
	//assert.Equals(t, stgs.Values(), stages_happy)

	t.Cleanup(func() {
		assert.NoError(t, running.Close())
	})
}

func TestProvider_DeployFail(t *testing.T) {
	logConfig := log.Config{
		Level:       log.LevelError,
		Destination: log.DestinationStdout,
	}
	logger := log.New(
		logConfig,
	)

	deploy_time_ms := 20
	workflow_deployer_cfg := map[string]any{
		"type":           "test-impl",
		"deploy_time":    deploy_time_ms,
		"deploy_succeed": true,
	}

	d_registry := deployer_registry.New(
		deployer.Any(testdeployer.NewFactory()))

	plp, err := plugin.New(
		logger,
		d_registry,
		workflow_deployer_cfg,
	)
	assert.NoError(t, err)

	runnable, err := plp.LoadSchema(
		map[string]any{"plugin": "simulation"}, map[string][]byte{})
	assert.NoError(t, err)

	handler := &deployFailStageChangeHandler{
		message: make(chan string),
	}

	// default step id
	running, err := runnable.Start(map[string]any{"step": nil}, handler)
	assert.NoError(t, err)

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		//map[string]any{"deploy": nil},
		map[string]any{"deploy": map[string]any{
			"type":           "test-impl",
			"deploy_succeed": false,
			"deploy_time":    deploy_time_ms}},
	))

	wait_time_ms := 50
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{"wait_time_ms": wait_time_ms}},
	))

	message := <-handler.message
	msg_expected := fmt.Sprintf("intentional deployment fail after %d ms", deploy_time_ms)
	assert.Equals(t, message, msg_expected)

	assert.Equals(t, string(running.State()),
		string(step.RunningStepStateFinished))

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDDeployFailed))

	//stages_exp := []string{
	//	string(plugin.StageIDDeploy),
	//	string(plugin.StageIDDeployFailed)}
	//
	//stgs := running.Stages()
	//assert.Equals(t, stgs.Values(), stages_exp)
}

func TestProvider_StartFail(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	deploy_time_ms := 20
	workflow_deployer_cfg := map[string]any{
		"type":           "test-impl",
		"deploy_time":    deploy_time_ms,
		"deploy_succeed": true,
	}

	plp, err := plugin.New(
		logger,
		deployer_registry.New(
			deployer.Any(testdeployer.NewFactory())),
		workflow_deployer_cfg,
	)
	assert.NoError(t, err)

	runnable, err := plp.LoadSchema(
		map[string]any{"plugin": "simulation"},
		map[string][]byte{})
	assert.NoError(t, err)

	handler := &runFailStageChangeHandler{
		message: make(chan string),
	}

	running, err := runnable.Start(map[string]any{"step": "wait"}, handler)
	assert.NoError(t, err)

	// tell deployer that this run should not succeed
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"type":                  "test-impl",
			"deploy_succeed":        true,
			"deploy_time":           deploy_time_ms,
			"disable_plugin_writes": true}},
	))

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{
			"wait_time_ms": 50}},
	))

	// wait for message, but we don't care about its value
	<-handler.message

	//stages_exp := []string{
	//	string(plugin.StageIDDeploy),
	//	string(plugin.StageIDStarting),
	//	//string(plugin.StageIDRunning),
	//	string(plugin.StageIDCrashed)}

	//stgs := running.Stages()
	//assert.Equals(t, stgs.Values(), stages_exp)

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDCrashed))
}

func TestProvider_RunFail(t *testing.T) {
	logger := log.New(
		log.Config{
			Level:       log.LevelError,
			Destination: log.DestinationStdout,
		},
	)
	deploy_time_ms := 20
	workflow_deployer_cfg := map[string]any{
		"type":           "test-impl",
		"deploy_time":    deploy_time_ms,
		"deploy_succeed": true,
	}

	plp, err := plugin.New(
		logger,
		deployer_registry.New(
			deployer.Any(testdeployer.NewFactory())),
		workflow_deployer_cfg,
	)
	assert.NoError(t, err)

	runnable, err := plp.LoadSchema(
		map[string]any{"plugin": "simulation"},
		map[string][]byte{})
	assert.NoError(t, err)

	handler := &runFailStageChangeHandler{
		message: make(chan string),
	}

	running, err := runnable.Start(map[string]any{"step": "wait"}, handler)
	assert.NoError(t, err)

	// tell deployer that this run should not succeed
	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDDeploy),
		map[string]any{"deploy": map[string]any{
			"type":           "test-impl",
			"deploy_succeed": true,
			"deploy_time":    deploy_time_ms}},
	))

	assert.NoError(t, running.ProvideStageInput(
		string(plugin.StageIDStarting),
		map[string]any{"input": map[string]any{
			"wait_time_ms":      50,
			"execution_succeed": false}},
	))

	// wait for message, but we don't care about its value
	<-handler.message

	//stages_exp := []string{
	//	string(plugin.StageIDDeploy),
	//	string(plugin.StageIDStarting),
	//	//string(plugin.StageIDRunning),
	//	string(plugin.StageIDCrashed)}

	//stgs := running.Stages()
	//assert.Equals(t, stgs.Values(), stages_exp)

	assert.Equals(t, running.CurrentStage(), string(plugin.StageIDCrashed))
}

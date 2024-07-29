package dummy_test

import (
	"fmt"
	"sync"
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/dummy"
)

type stageChangeHandler struct {
	message chan string
}

func (s *stageChangeHandler) OnStepStageFailure(_ step.RunningStep, _ string, _ *sync.WaitGroup, _ error) {
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
	if previousStage != "greet" {
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
	message := (*previousStageOutput).(map[string]any)["message"].(string)
	s.message <- message
}

func TestProvider(t *testing.T) {
	provider := dummy.New()
	assert.Equals(t, provider.Kind(), "dummy")
	runnable, err := provider.LoadSchema(map[string]any{}, map[string][]byte{})
	assert.NoError(t, err)

	handler := &stageChangeHandler{
		message: make(chan string),
	}

	running, err := runnable.Start(map[string]any{}, t.Name(), handler)
	assert.NoError(t, err)
	assert.NoError(t, running.ProvideStageInput("greet", map[string]any{
		"name": "Arca Lot",
	}))

	message := <-handler.message
	assert.Equals(t, message, "Hello Arca Lot!")
}

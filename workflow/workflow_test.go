package workflow_test

import (
	"context"
	"errors"
	"testing"

	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

var workflowDefinition = `
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
	stepRegistry := lang.Must2(registry.New(
		dummy.New(),
	))
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(workflowDefinition)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{}))
	_, outputData, err := preparedWorkflow.Execute(context.Background(), map[string]any{"name": "Arca Lot"})
	assert.Nil(t, outputData)
	assert.Error(t, err)
	var typedError *workflow.ErrNoMorePossibleSteps
	if !errors.As(err, &typedError) {
		t.Fatalf("incorrect error type returned: %T", err)
	}
}

package foreach_test

import (
	"context"
	"fmt"
	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/builtinfunctions"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/foreach"
	"go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

// mainWorkflow is the workflow calling the foreach step.
var mainWorkflow = `
version: v0.2.0
input:
  root: names
  objects:
    names:
      id: names
      properties:
        names:
          type:
            type_id: list
            items:
              type_id: object
              id: name
              properties:
                name:
                  type:
                    type_id: string
steps:
  greet:
    kind: foreach
    items: !expr $.input.names
    workflow: subworkflow.yaml
    parallelism: 5
outputs:
  success:
    messages: !expr $.steps.greet.outputs.success.data
  failed:
    error: !expr $.steps.greet.failed.error
`

var subworkflow = `
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
outputs:
  success:
    message: !expr $.steps.say_hi.greet.success.message
  error:
    reason: !expr $.steps.say_hi.greet.error.reason
`

// ExampleNew provides an example for using the foreach provider to run subworkflows.
func ExampleNew() {
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
	factories := workflowFactory{
		config: cfg,
	}
	stepRegistry := lang.Must2(registry.New(
		dummy.New(),
		lang.Must2(foreach.New(logger, factories.createYAMLParser, factories.createWorkflow)),
	))
	factories.stepRegistry = stepRegistry
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
		builtinfunctions.GetFunctions(),
	))
	wf := lang.Must2(workflow.NewYAMLConverter(stepRegistry).FromYAML([]byte(mainWorkflow)))
	preparedWorkflow := lang.Must2(executor.Prepare(wf, map[string][]byte{
		"subworkflow.yaml": []byte(subworkflow),
	}))
	outputID, outputData := lang.Must3(preparedWorkflow.Execute(context.Background(), map[string]any{
		"names": []any{
			map[string]any{
				"name": "Arca",
			},
			map[string]any{
				"name": "Lot",
			},
		},
	},
	))
	if outputID != "success" {
		panic(fmt.Errorf("workflow run failed"))
	}
	data := outputData.(map[any]any)["messages"].([]any)
	for _, entry := range data {
		fmt.Printf("%s ", entry.(map[any]any)["message"])
	}
	fmt.Println()
	// Output: Hello Arca! Hello Lot!
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

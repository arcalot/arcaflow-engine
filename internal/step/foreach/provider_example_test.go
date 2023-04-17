package foreach_test

import (
	"context"
	"fmt"

	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/foreach"
	"go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

// mainWorkflow is the workflow calling the foreach step.
var mainWorkflow = `
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
output:
  messages: !expr $.steps.greet.outputs.success.data
`

var subworkflow = `
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
  message: !expr $.steps.say_hi.greet.success.message
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
	stepRegistry := lang.Must2(registry.New(
		dummy.New(),
		lang.Must2(foreach.New(logger, cfg)),
	))
	executor := lang.Must2(workflow.NewExecutor(
		logger,
		cfg,
		stepRegistry,
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

package workflow_test

import (
	"context"
	"fmt"
	"go.flow.arcalot.io/engine/config"

	"go.arcalot.io/lang"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/registry"
	"go.flow.arcalot.io/engine/workflow"
)

var workflowYAML = `---
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
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

func ExampleExecutor() {
	logger := log.NewLogger(log.LevelDebug, log.NewGoLogWriter())
	stepRegistry := lang.Must2(registry.New(
		dummy.New(),
	))

	executor, err := workflow.NewExecutor(logger, &config.Config{}, stepRegistry)
	if err != nil {
		panic(err)
	}

	yamlConverter := workflow.NewYAMLConverter(stepRegistry)
	decodedWorkflow, err := yamlConverter.FromYAML([]byte(workflowYAML))
	if err != nil {
		panic(err)
	}

	preparedWorkflow, err := executor.Prepare(decodedWorkflow, nil)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outputData, err := preparedWorkflow.Execute(ctx, map[string]any{
		"name": "Arca Lot",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(outputData.(map[any]any)["message"])
	// Output: Hello Arca Lot!
}

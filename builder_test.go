package engine //nolint:testpackage
import (
	"strings"
	"testing"

	"go.arcalot.io/assert"
	"go.arcalot.io/dgraph"
	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/schema"
)

func TestAddDependencies_SingleNode(t *testing.T) {
	dag := dgraph.New[treeItem]()

	ti := treeItem{"steps", "step-1", ""}
	startingStep, err := dag.AddNode("steps.step-1", ti)
	assert.NoError(t, err)

	yamlNode, err := yaml.New().Parse([]byte(`test`))
	assert.NoError(t, err)

	assert.NoErrorR[any](t)(addDependencies(startingStep, yamlNode, map[treeItem]dgraph.Node[treeItem]{
		ti: startingStep,
	}))
}

func TestAddDependencies_SingleNodeCycle(t *testing.T) {
	dag := dgraph.New[treeItem]()

	// Define the step node
	ti1 := treeItem{"steps", "step-1", ""}
	startingStep, err := dag.AddNode("steps.step-1", ti1)
	assert.NoError(t, err)
	// Define the output node
	ti2 := treeItem{"steps", "step-1", "success"}
	outputStep, err := dag.AddNode("steps.step-1.outputs.success", ti2)
	assert.NoError(t, err)
	// Connect the step to its output node.
	assert.NoError(t, outputStep.Connect(startingStep.ID()))

	_, err = yaml.New().Parse([]byte(`!expr $.steps["step-1"].outputs.success`))
	assert.NoError(t, err)
}

//nolint:dupl
func TestAddDependencies_TwoNodes(t *testing.T) {
	dag := dgraph.New[treeItem]()

	ti1 := treeItem{"steps", "step1", "success"}
	startingStep, err := dag.AddNode("steps.step1.outputs.success", ti1)
	assert.NoError(t, err)
	ti2 := treeItem{"steps", "step2", ""}
	step2, err := dag.AddNode("steps.step2", ti2)
	assert.NoError(t, err)

	yamlNode, err := yaml.New().Parse([]byte(`!expr $.steps.step1.outputs.success`))
	assert.NoError(t, err)

	assert.NoErrorR[any](t)(addDependencies(step2, yamlNode, map[treeItem]dgraph.Node[treeItem]{
		ti1: startingStep,
		ti2: step2,
	}))

	node1Out, err := startingStep.ListOutboundConnections()
	assert.NoError(t, err)
	assert.Equals(t, len(node1Out), 1)
	assert.Equals(t, node1Out["steps.step2"].ID(), "steps.step2")
}

//nolint:dupl
func TestAddDependencies_TwoNodesMapAccessor(t *testing.T) {
	dag := dgraph.New[treeItem]()

	ti1 := treeItem{"steps", "step-1", "success"}
	startingStep, err := dag.AddNode("steps.step-1.outputs.success", ti1)
	assert.NoError(t, err)
	ti2 := treeItem{"steps", "step-2", ""}
	step2, err := dag.AddNode("steps.step-2", ti2)
	assert.NoError(t, err)

	yamlNode, err := yaml.New().Parse([]byte(`!expr $.steps["step-1"].outputs.success`))
	assert.NoError(t, err)

	assert.NoErrorR[any](t)(addDependencies(step2, yamlNode, map[treeItem]dgraph.Node[treeItem]{
		ti1: startingStep,
		ti2: step2,
	}))

	node1Out, err := startingStep.ListOutboundConnections()
	assert.NoError(t, err)
	assert.Equals(t, len(node1Out), 1)
	assert.Equals(t, node1Out["steps.step-2"].ID(), "steps.step-2")
}

//nolint:dupl
func TestAddDependencies_TwoNodesMapAccessorFirst(t *testing.T) {
	dag := dgraph.New[treeItem]()

	ti1 := treeItem{"steps", "step1", "success"}
	startingStep, err := dag.AddNode("steps.step1.outputs.success", ti1)
	assert.NoError(t, err)
	ti2 := treeItem{"steps", "step2", ""}
	step2, err := dag.AddNode("steps.step2", ti2)
	assert.NoError(t, err)

	yamlNode, err := yaml.New().Parse([]byte(`!expr $["steps"].step1.outputs.success`))
	assert.NoError(t, err)

	assert.NoErrorR[any](t)(addDependencies(step2, yamlNode, map[treeItem]dgraph.Node[treeItem]{
		ti1: startingStep,
		ti2: step2,
	}))

	node1Out, err := startingStep.ListOutboundConnections()
	assert.NoError(t, err)
	assert.Equals(t, len(node1Out), 1)
	assert.Equals(t, node1Out["steps.step2"].ID(), "steps.step2")
}

func TestBuildDependencyTree_empty(t *testing.T) {
	output, err := yaml.New().Parse([]byte("{}"))
	assert.NoError(t, err)
	dependencyTree, data, _, err := buildDependencyTree(schema.NewScopeSchema(
		schema.NewObjectSchema(
			"test",
			map[string]*schema.PropertySchema{},
		),
	), map[string]*workflow.PluginStep{}, map[string]schema.Step{}, output)
	assert.NoError(t, err)

	assert.Equals(t, len(dependencyTree.ListNodesWithoutInboundConnections()), 1)
	assert.Equals(t, dependencyTree.HasCycles(), false)
	assert.Equals(t, data, map[string]*workflow.PluginStep{})
}

//nolint:funlen
func TestBuildDependencyTree_one(t *testing.T) {
	parsedDeploy, err := yaml.New().Parse([]byte(`type: docker`))
	assert.NoError(t, err)
	parsedInput, err := yaml.New().Parse([]byte(`foo: bar`))
	assert.NoError(t, err)
	output, err := yaml.New().Parse([]byte("{}"))
	assert.NoError(t, err)
	dependencyTree, data, _, err := buildDependencyTree(schema.NewScopeSchema(
		schema.NewObjectSchema(
			"test",
			map[string]*schema.PropertySchema{},
		),
	), map[string]*workflow.PluginStep{
		"step-1": {
			Input:  parsedInput,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
	}, map[string]schema.Step{
		"step-1": &schema.StepSchema{
			IDValue: "step-1",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
	}, output)
	assert.NoError(t, err)

	startedNodes := dependencyTree.ListNodesWithoutInboundConnections()
	assert.Equals(t, len(startedNodes), 2)
	assert.Equals(t, startedNodes["steps.step-1"].ID(), "steps.step-1")
	assert.Equals(t, dependencyTree.HasCycles(), false)
	assert.Equals(t, data["step-1"].Deploy.(map[any]any), map[any]any{"type": "docker"})
	assert.Equals(t, data["step-1"].Input.(map[any]any), map[any]any{"foo": "bar"})
}

//nolint:funlen
func TestBuildDependencyTree_two(t *testing.T) {
	parsedDeploy, err := yaml.New().Parse([]byte(`type: docker`))
	assert.NoError(t, err)
	parsedInput1, err := yaml.New().Parse([]byte(`foo: bar`))
	assert.NoError(t, err)
	parsedInput2, err := yaml.New().Parse([]byte(`foo: !expr $.steps["step-1"].outputs.success.foo`))
	assert.NoError(t, err)
	output, err := yaml.New().Parse([]byte("{}"))
	assert.NoError(t, err)
	//nolint:dupl
	dependencyTree, data, _, err := buildDependencyTree(schema.NewScopeSchema(
		schema.NewObjectSchema(
			"test",
			map[string]*schema.PropertySchema{},
		),
	), map[string]*workflow.PluginStep{
		"step-1": {
			Input:  parsedInput1,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
		"step-2": {
			Input:  parsedInput2,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
	}, map[string]schema.Step{
		"step-1": &schema.StepSchema{
			IDValue: "step-1",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
		"step-2": &schema.StepSchema{
			IDValue: "step-2",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
	}, output)
	assert.NoError(t, err)

	t.Logf("Mermaid diagram:\n%s", dependencyTree.Mermaid())

	startedNodes := dependencyTree.ListNodesWithoutInboundConnections()
	assert.Equals(t, len(startedNodes), 2)
	assert.Equals(t, startedNodes["steps.step-1"].ID(), "steps.step-1")
	assert.Equals(t, dependencyTree.HasCycles(), false)
	assert.Equals(t, data["step-1"].Deploy.(map[any]any), map[any]any{"type": "docker"})
	assert.Equals(t, data["step-1"].Input.(map[any]any), map[any]any{"foo": "bar"})
	_, isAstNode := data["step-2"].Input.(map[any]any)["foo"].(expressions.ASTNode)
	assert.Equals(t, isAstNode, true)

	step2, err := dependencyTree.GetNodeByID("steps.step-2")
	assert.NoError(t, err)

	step2Connections, err := step2.ListInboundConnections()
	assert.NoError(t, err)
	assert.Equals(t, len(step2Connections), 1)
	assert.Equals(t, step2Connections["steps.step-1.outputs.success"].ID(), "steps.step-1.outputs.success")
}

//nolint:funlen
func TestBuildDependencyTree_invalid_output(t *testing.T) {
	parsedDeploy, err := yaml.New().Parse([]byte(`type: docker`))
	assert.NoError(t, err)
	parsedInput1, err := yaml.New().Parse([]byte(`foo: bar`))
	assert.NoError(t, err)
	parsedInput2, err := yaml.New().Parse([]byte(`foo: !expr $.steps["step-1"].outputs.nonexistent`))
	assert.NoError(t, err)
	//nolint:dupl,dogsled
	_, _, _, err = buildDependencyTree(schema.NewScopeSchema(
		schema.NewObjectSchema(
			"test",
			map[string]*schema.PropertySchema{},
		),
	), map[string]*workflow.PluginStep{
		"step-1": {
			Input:  parsedInput1,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
		"step-2": {
			Input:  parsedInput2,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
	}, map[string]schema.Step{
		"step-1": &schema.StepSchema{
			IDValue: "step-1",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
		"step-2": &schema.StepSchema{
			IDValue: "step-2",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
	}, nil)
	assert.Error(t, err)
}

//nolint:funlen
func TestBuildDependencyTree_two_cycle(t *testing.T) {
	parsedDeploy, err := yaml.New().Parse([]byte(`type: docker`))
	assert.NoError(t, err)
	parsedInput1, err := yaml.New().Parse([]byte(`foo: !expr $.steps["step-2"].outputs.success`))
	assert.NoError(t, err)
	parsedInput2, err := yaml.New().Parse([]byte(`foo: !expr $.steps["step-1"].outputs.success`))
	assert.NoError(t, err)
	output, err := yaml.New().Parse([]byte("{}"))
	assert.NoError(t, err)

	//nolint:dupl
	depTree, _, _, err := buildDependencyTree(schema.NewScopeSchema(
		schema.NewObjectSchema(
			"test",
			map[string]*schema.PropertySchema{},
		),
	), map[string]*workflow.PluginStep{
		"step-1": {
			Input:  parsedInput1,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
		"step-2": {
			Input:  parsedInput2,
			Plugin: "quay.io/arcalot/test",
			Deploy: parsedDeploy,
		},
	}, map[string]schema.Step{
		"step-1": &schema.StepSchema{
			IDValue: "step-1",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
		"step-2": &schema.StepSchema{
			IDValue: "step-2",
			InputValue: &schema.ScopeSchema{
				ObjectsValue: map[string]*schema.ObjectSchema{
					"root": {
						IDValue: "root",
						PropertiesValue: map[string]*schema.PropertySchema{
							"foo": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					},
				},
				RootValue: "root",
			},
			OutputsValue: map[string]*schema.StepOutputSchema{
				"success": {
					SchemaValue: &schema.ScopeSchema{
						ObjectsValue: map[string]*schema.ObjectSchema{
							"root": {
								IDValue: "root",
								PropertiesValue: map[string]*schema.PropertySchema{
									"foo": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							},
						},
						RootValue: "root",
					},
				},
			},
			DisplayValue: nil,
		},
	}, output)
	assert.NoError(t, err)
	assert.Equals(t, depTree.HasCycles(), true)
}

func TestBuildExpressionSelector(t *testing.T) {
	for name, tc := range map[string]struct {
		input          string
		expectedOutput string
		error          bool
	}{
		"single": {
			"$.steps",
			"$.steps",
			false,
		},
		"double": {
			"$.steps.foo",
			"$.steps.foo",
			false,
		},
		"map1": {
			"$[\"steps\"].foo",
			"$.steps.foo",
			false,
		},
		"map2": {
			"$.steps[\"foo\"]",
			"$.steps.foo",
			false,
		},
	} {
		tc := tc
		t.Run(name, func(t *testing.T) {
			parser, err := expressions.InitParser(tc.input, "workflow.yaml")
			assert.NoError(t, err)
			expr, err := parser.ParseExpression()
			assert.NoError(t, err)
			stack := &[]string{}
			err = buildExpressionSelector(expr, stack)
			if tc.error {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equals(t, strings.Join(*stack, "."), tc.expectedOutput)
			}

		})
	}
}

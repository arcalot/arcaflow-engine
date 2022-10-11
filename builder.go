package engine

import (
	"errors"
	"fmt"

	"go.arcalot.io/lang"
	"go.flow.arcalot.io/engine/internal/dg"
	"go.flow.arcalot.io/engine/internal/expressions"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// treeItem is an item in the workflow dependency tree.
type treeItem struct {
	Type   string
	Item   string
	Output string
}

const yamlTypeExpr = "!expr"
const stepsKey = "steps"
const outputsKey = "outputs"
const inputKey = "input"

// buildDependencyTree builds a dependency tree from the input schema and steps. The function expects the input and
// deploy parameters to be prepared as yaml.Node instances. The returned steps contain the resolved raw data and the
// AST nodes if an expression was found.
//nolint:funlen,gocognit
func buildDependencyTree(inputSchema *schema.ScopeSchema, steps map[string]*workflow.PluginStep, stepSchemas map[string]schema.Step, output yaml.Node) (dg.DirectedGraph[treeItem], map[string]*workflow.PluginStep, any, error) {
	dag := dg.New[treeItem]()
	selectableItems := map[treeItem]dg.Node[treeItem]{}
	for propertyID := range inputSchema.Properties() {
		nodeID := fmt.Sprintf("%s.%s", inputKey, propertyID)
		item := treeItem{inputKey, propertyID, ""}
		n, err := dag.AddNode(nodeID, item)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to add node %s to the dependency tree (%w)", nodeID, err)
		}
		selectableItems[item] = n
	}
	for stepID := range steps {
		nodeID := fmt.Sprintf("%s.%s", stepsKey, stepID)
		item := treeItem{stepsKey, stepID, ""}
		n, err := dag.AddNode(nodeID, item)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to add node %s to the dependency tree (%w)", nodeID, err)
		}

		for outputID := range stepSchemas[stepID].Outputs() {
			nodeID := fmt.Sprintf("%s.%s.%s.%s", stepsKey, stepID, outputsKey, outputID)
			item := treeItem{stepsKey, stepID, outputID}
			no, err := dag.AddNode(nodeID, item)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to add node %s to the dependency tree (%w)", nodeID, err)
			}
			if err := n.Connect(no.ID()); err != nil {
				decodedErr := &dg.ErrConnectionAlreadyExists{}
				if !errors.As(err, &decodedErr) {
					return nil, nil, nil, fmt.Errorf(
						"failed to connect node %s to node %s in the dependency tree (%w)",
						nodeID,
						n.ID(),
						err,
					)
				}
			}
			selectableItems[item] = no
		}
	}

	for stepID, step := range steps {
		currentNode, err := dag.GetNodeByID(fmt.Sprintf("%s.%s", stepsKey, stepID))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("bug: current node %s is not in the DAG (%w)", stepID, err)
		}

		// Deploy parameters
		deployYaml, ok := step.Deploy.(yaml.Node)
		if !ok {
			if deployYaml != nil {
				return nil, nil, nil, fmt.Errorf("bug: the step %s deploy is not a YAML node", stepID)
			}
			deployYaml = lang.Must2(yaml.New().Parse([]byte("{}")))
		}
		if step.Deploy, err = addDependencies(currentNode, deployYaml, selectableItems); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to process step %s (%w)", stepID, err)
		}

		// Input parameters
		inputYaml, ok := step.Input.(yaml.Node)
		if !ok {
			return nil, nil, nil, fmt.Errorf("bug: the step %s input is not a YAML node", stepID)
		}
		if step.Input, err = addDependencies(currentNode, inputYaml, selectableItems); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to process step %s (%w)", stepID, err)
		}
		steps[stepID] = step
	}
	outputNode, err := dag.AddNode("output", treeItem{
		Type:   "output",
		Item:   "",
		Output: "",
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to add output node (%w)", err)
	}
	outputData, err := addDependencies(outputNode, output, selectableItems)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to process output data (%w)", err)
	}
	return dag, steps, outputData, nil
}

// addDependencies adds the dependencies based on the expression AST. If the AST parsing succeeds, the AST is returned.
// If no expression is found, the raw data is returned.
//nolint:funlen,gocognit
func addDependencies(
	node dg.Node[treeItem],
	data yaml.Node,
	selectableItems map[treeItem]dg.Node[treeItem],
) (any, error) {
	ast, err := tryParseExpression(data)
	if err != nil {
		return nil, err
	}
	if ast == nil {
		switch data.Type() {
		case yaml.TypeIDMap:
			result := make(map[any]any, len(data.Contents())/2)
			for i := 0; i < len(data.Contents()); i += 2 {
				key := data.Contents()[i].Raw()
				value, err := addDependencies(node, data.Contents()[i+1], selectableItems)
				if err != nil {
					return nil, err
				}
				result[key] = value
			}
			return result, nil
		case yaml.TypeIDSequence:
			result := make([]any, len(data.Contents()))
			for i := 0; i < len(data.Contents()); i++ {
				value, err := addDependencies(node, data.Contents()[i], selectableItems)
				if err != nil {
					return nil, err
				}
				result[i] = value
			}
			return result, nil
		case yaml.TypeIDString:
			return data.Value(), nil
		default:
			return nil, fmt.Errorf("unsupported YAML node type: %s", data.Type())
		}
	}

	stack := []string{}

	if err := buildExpressionSelector(ast, &stack); err != nil {
		return nil, err
	}

	// Drop the starting $
	stack = stack[1:]
	if len(stack) < 2 || (stack[0] == stepsKey && len(stack) < 4) {
		return nil, fmt.Errorf("expression too short, please write a more specific expression")
	}
	items := map[treeItem]dg.Node[treeItem]{}
	for k, v := range selectableItems {
		if v.Item().Type != stack[0] {
			continue
		}
		if v.Item().Item != stack[1] {
			continue
		}
		if stack[0] == stepsKey {
			if stack[2] != outputsKey {
				return nil, fmt.Errorf("invalid expression, only outputs can be selected: %s", data.Value())
			}
			if stack[3] != v.Item().Output {
				continue
			}
		}
		items[k] = v
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("invalid expression, nothing selected: %s", data.Value())
	}

	for _, v := range items {
		if err := v.Connect(node.ID()); err != nil {
			decodedErr := &dg.ErrConnectionAlreadyExists{}
			if !errors.As(err, &decodedErr) {
				return nil, err
			}
		}
	}
	return ast, nil
}

//nolint:funlen,gocognit
func buildExpressionSelector(node expressions.ASTNode, stack *[]string) error {
	if len(*stack) > 1 {
		if (*stack)[1] == inputKey && len(*stack) > 2 {
			return nil
		}
		if (*stack)[1] == stepsKey && len(*stack) > 4 {
			return nil
		}
	}
	if node.Left() != nil {
		if err := buildExpressionSelector(node.Left(), stack); err != nil {
			return err
		}
	}
	if len(*stack) == 0 {
		id, ok := node.(*expressions.Identifier)
		if !ok || id.IdentifierName != "$" {
			return fmt.Errorf("expressions must begin with $")
		}
		*stack = append(*stack, "$")
		return nil
	}
	switch n := node.(type) {
	case *expressions.DotNotation:
	case *expressions.MapAccessor:
		key, ok := n.Right().(*expressions.Key)
		if !ok {
			return fmt.Errorf(
				"only key expressions are supported in map accessors on the first two levels of input expressions and first four levels of step output expressions, %T found",
				node.Right(),
			)
		}
		if key.SubExpression != nil {
			return fmt.Errorf("subexpressions are not supported in map accessors on the first two levels of expressions")
		}
		val := key.Literal.Value()
		switch v := val.(type) {
		case int:
			*stack = append(*stack, fmt.Sprintf("%d", v))
		case string:
			*stack = append(*stack, v)
		default:
			return fmt.Errorf("unsupported map accesor type: %T", val)
		}

		return nil
	case *expressions.Key:
	case *expressions.Identifier:
		switch {
		case n.IdentifierName == "$":
			return fmt.Errorf("the $ identifier can only be used at the beginning of an expression")
		default:
			*stack = append(*stack, n.IdentifierName)
		}
	default:
		return fmt.Errorf("unsupported AST node type: %T", n)
	}
	if node.Right() != nil {
		if err := buildExpressionSelector(node.Right(), stack); err != nil {
			return err
		}
	}
	return nil
}

// tryParseExpression attempts to identify an expression in a YAML node. If one is found, the expression is compiled and
// returned. Otherwise, nil is returned. If the expression parsing fails, an error is returned.
func tryParseExpression(data yaml.Node) (expressions.ASTNode, error) {
	if data.Tag() != yamlTypeExpr {
		return nil, nil
	}
	parser, err := expressions.InitParser(data.Value(), "workflow.yaml")
	if err != nil {
		return nil, err
	}
	ast, err := parser.ParseExpression()
	if err != nil {
		return nil, err
	}
	return ast, nil
}

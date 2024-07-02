package workflow

import (
	"fmt"
	"strings"

	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/expressions"
)

// YAMLConverter converts a raw YAML into a usable workflow.
type YAMLConverter interface {
	FromYAML(data []byte) (*Workflow, error)
}

// NewYAMLConverter creates a YAMLConverter.
func NewYAMLConverter(stepRegistry step.Registry) YAMLConverter {
	return &yamlConverter{
		stepRegistry: stepRegistry,
	}
}

type yamlConverter struct {
	stepRegistry step.Registry
}

func (y yamlConverter) FromYAML(data []byte) (*Workflow, error) {
	if len(data) == 0 {
		return nil, ErrEmptyWorkflowFile
	}

	yamlParser := yaml.New()
	parsedData, err := yamlParser.Parse(data)
	if err != nil {
		return nil, &ErrInvalidWorkflowYAML{err}
	}

	rawWorkflow, err := yamlBuildExpressions(parsedData, []string{})
	if err != nil {
		return nil, &ErrInvalidWorkflow{err}
	}

	workflowSchema := GetSchema()
	workflow, err := workflowSchema.UnserializeType(rawWorkflow)
	if err != nil {
		return nil, &ErrInvalidWorkflow{err}
	}
	return workflow, nil
}

func yamlBuildExpressions(data yaml.Node, path []string) (any, error) {
	if data.Tag() == "!expr" {
		if data.Type() != yaml.TypeIDString {
			return nil, fmt.Errorf("!!expr found on non-string node at %s", strings.Join(path, " -> "))
		}
		expr, err := expressions.New(data.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to compile expression at %s (%w)", strings.Join(path, " -> "), err)
		}
		return expr, nil
	}
	switch data.Type() {
	case yaml.TypeIDString:
		return data.Value(), nil
	case yaml.TypeIDMap:
		result := make(map[string]any, len(data.MapKeys()))
		mapkeys := data.MapKeys()
		for _, key := range mapkeys {
			node, _ := data.MapKey(key)
			var err error
			result[key], err = yamlBuildExpressions(node, append(path, key))
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case yaml.TypeIDSequence:
		result := make([]any, len(data.Contents()))
		for i, node := range data.Contents() {
			var err error
			result[i], err = yamlBuildExpressions(node, append(path, fmt.Sprintf("%d", i)))
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("invalid YAML node type: %s", data.Type())
	}
}

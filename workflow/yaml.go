package workflow

import (
	"fmt"
	"go.flow.arcalot.io/engine/internal/infer"
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

// YamlOneOfKey is the key to specify the oneof options within a !oneof section.
const YamlOneOfKey = "one_of"

// YamlDiscriminatorKey is the key to specify the discriminator inside a !oneof section.
const YamlDiscriminatorKey = "discriminator"

// YamlOneOfTag is the yaml tag that allows the section to be interpreted as a OneOf.
const YamlOneOfTag = "!oneof"

func buildOneOfExpressions(data yaml.Node, path []string) (any, error) {
	if data.Type() != yaml.TypeIDMap {
		return nil, fmt.Errorf(
			"!oneof found on non-map node at %s; expected a map with a list of options and the discriminator ",
			strings.Join(path, " -> "))
	}
	discriminatorNode, found := data.MapKey(YamlDiscriminatorKey)
	if !found {
		return nil, fmt.Errorf("key %q not present within %s at %q",
			YamlDiscriminatorKey, YamlOneOfTag, strings.Join(path, " -> "))
	}
	if discriminatorNode.Type() != yaml.TypeIDString {
		return nil, fmt.Errorf("%q within %s should be a string; got %s",
			YamlDiscriminatorKey, YamlOneOfTag, discriminatorNode.Type())
	}
	discriminator := discriminatorNode.Value()
	if len(discriminator) == 0 {
		return nil, fmt.Errorf("%q within %s is empty", YamlDiscriminatorKey, YamlOneOfTag)
	}
	oneOfOptionsNode, found := data.MapKey(YamlOneOfKey)
	if !found {
		return nil, fmt.Errorf("key %q not present within %s at %q",
			YamlOneOfKey, YamlOneOfTag, strings.Join(path, " -> "))
	}
	if oneOfOptionsNode.Type() != yaml.TypeIDMap {
		return nil, fmt.Errorf("%q within %q should be a map; got %s",
			YamlOneOfKey, YamlOneOfTag, discriminatorNode.Type())
	}
	options := map[string]any{}
	for _, optionNodeKey := range oneOfOptionsNode.MapKeys() {
		optionNode, _ := oneOfOptionsNode.MapKey(optionNodeKey)
		var err error
		options[optionNodeKey], err = yamlBuildExpressions(optionNode, append(path, optionNodeKey))
		if err != nil {
			return nil, err
		}
	}

	return &infer.OneOfExpression{
		Discriminator: discriminator,
		Options:       options,
	}, nil
}

func yamlBuildExpressions(data yaml.Node, path []string) (any, error) {
	switch data.Tag() {
	case "!expr":
		if data.Type() != yaml.TypeIDString {
			return nil, fmt.Errorf("!expr found on non-string node at %s", strings.Join(path, " -> "))
		}
		expr, err := expressions.New(data.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to compile expression at %s (%w)", strings.Join(path, " -> "), err)
		}
		return expr, nil
	case YamlOneOfTag:
		return buildOneOfExpressions(data, path)
	}
	switch data.Type() {
	case yaml.TypeIDString:
		return data.Value(), nil
	case yaml.TypeIDMap:
		result := make(map[string]any, len(data.MapKeys()))
		for _, key := range data.MapKeys() {
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

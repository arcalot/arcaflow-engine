package workflow //nolint:testpackage // Tests internal functions for unit testing.

import (
	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.flow.arcalot.io/engine/internal/infer"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/expressions"
	"testing"
)

func TestBuildOneOfExpression_Simple(t *testing.T) {
	yamlInput := []byte(`
!oneof
  discriminator: d
  one_of:
    a: !expr some_expr
    b: !expr some_other_expr
`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	result, err := buildOneOfExpressions(input, make([]string, 0))
	assert.NoError(t, err)
	assert.InstanceOf[*infer.OneOfExpression](t, result)
	oneofResult := result.(*infer.OneOfExpression)
	assert.Equals(t, oneofResult.Discriminator, "d")
	assert.Equals(t, oneofResult.Options, map[string]any{
		"a": lang.Must2(expressions.New("some_expr")),
		"b": lang.Must2(expressions.New("some_other_expr")),
	})
}

func TestBuildOneOfExpression_InputValidation(t *testing.T) {
	// Not a map
	yamlInput := []byte(`
!oneof "thisisastring"`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err := buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected a map")

	// Wrong discriminator key
	yamlInput = []byte(`
!oneof
  wrong_key: ""
  one_of: {}`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key \""+YamlDiscriminatorKey+"\" not present")

	// Discriminator not a string
	yamlInput = []byte(`
!oneof
  discriminator: {}
  one_of: {}`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "should be a string")

	// Empty discriminator
	yamlInput = []byte(`
!oneof
  discriminator: ""
  one_of: {}`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is empty")

	// Missing or wrong oneof key
	yamlInput = []byte(`
!oneof
  discriminator: "valid"
  one_of_wrong: {}`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key \"one_of\" not present")

	// Wrong type for oneof options node
	yamlInput = []byte(`
!oneof
  discriminator: "valid"
  one_of: wrong`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildOneOfExpressions(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "should be a map")

	// Non-object type as option in one_of section.
	yamlInput = []byte(`
!oneof
  discriminator: "valid"
  one_of:
    a: test`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	oneofResult, err := buildOneOfExpressions(input, make([]string, 0))
	assert.NoError(t, err)
	assert.InstanceOf[*infer.OneOfExpression](t, oneofResult)
	_, err = oneofResult.(*infer.OneOfExpression).Type(nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not an object")

}

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

func TestBuildResultOrDisabledExpression_Simple(t *testing.T) {
	// Test without root $
	yamlInput := []byte(`!ordisabled steps.test.outputs`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	oneOfResult, err := buildResultOrDisabledExpression(input, make([]string, 0))
	assert.NoError(t, err)
	assert.Equals(t, oneOfResult.Discriminator, "result")
	assert.Equals(t, oneOfResult.Options, map[string]any{
		"enabled":  lang.Must2(expressions.New("steps.test.outputs")),
		"disabled": lang.Must2(expressions.New("steps.test.disabled.output")),
	})

	// Test with all outputs
	yamlInput = []byte(`!ordisabled $.steps.test.outputs`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	oneOfResult, err = buildResultOrDisabledExpression(input, make([]string, 0))
	assert.NoError(t, err)
	assert.Equals(t, oneOfResult.Discriminator, "result")
	assert.Equals(t, oneOfResult.Options, map[string]any{
		"enabled":  lang.Must2(expressions.New("$.steps.test.outputs")),
		"disabled": lang.Must2(expressions.New("$.steps.test.disabled.output")),
	})

	// Test with a specific output
	yamlInput = []byte(`!ordisabled $.steps.test.outputs.success`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	oneOfResult, err = buildResultOrDisabledExpression(input, make([]string, 0))
	assert.NoError(t, err)
	assert.Equals(t, oneOfResult.Discriminator, "result")
	assert.Equals(t, oneOfResult.Options, map[string]any{
		"enabled":  lang.Must2(expressions.New("$.steps.test.outputs.success")),
		"disabled": lang.Must2(expressions.New("$.steps.test.disabled.output")),
	})
}

func TestBuildResultOrDisabledExpression_InvalidPattern(t *testing.T) {
	// Missing the output
	yamlInput := []byte(`!ordisabled $.steps.test`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err := buildResultOrDisabledExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to parse expression")
	// Trailing period. This could either trigger an unable to parse expression error
	// or a token not found error depending on the order of the function under test.
	yamlInput = []byte(`!ordisabled $.steps.test.`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildResultOrDisabledExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token not found")
	// Misspelled steps
	yamlInput = []byte(`!ordisabled $.stepswrong.test`)
	input = assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err = buildResultOrDisabledExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to parse expression")
}

func TestBuildExpression_WrongType(t *testing.T) {
	yamlInput := []byte(`!expr {}`) // A map
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err := buildExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "found on non-string node")
}

func TestBuildWaitOptionalExpr_Simple(t *testing.T) {
	yamlInput := []byte(`!wait-optional some_expr`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	optionalResult, err := buildOptionalExpression(input, make([]string, 0))
	assert.NoError(t, err)
	assert.Equals(t, optionalResult.WaitForCompletion, true)
	assert.Equals(t, optionalResult.Expr, lang.Must2(expressions.New("some_expr")))
}

func TestBuildSoftOptionalExpr_Simple(t *testing.T) {
	yamlInput := []byte(`!soft-optional some_expr`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	optionalResult, err := buildOptionalExpression(input, make([]string, 0))
	assert.NoError(t, err)
	assert.Equals(t, optionalResult.WaitForCompletion, false)
	assert.Equals(t, optionalResult.Expr, lang.Must2(expressions.New("some_expr")))
}

func TestBuildWaitOptionalExpr_InvalidExpr(t *testing.T) {
	yamlInput := []byte(`!wait-optional ....`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err := buildOptionalExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "placed in invalid configuration")
}

func TestBuildWaitOptionalExpr_InvalidTag(t *testing.T) {
	// This code tests an invalid tag used with buildOptionalExpressions that
	// should not be possible in production code due to other checks.
	yamlInput := []byte(`!invalid some_expr`)
	input := assert.NoErrorR[yaml.Node](t)(yaml.New().Parse(yamlInput))
	_, err := buildOptionalExpression(input, make([]string, 0))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported tag")
}

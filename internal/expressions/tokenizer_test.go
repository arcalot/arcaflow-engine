package expressions_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/expressions"
)

func TestTokenizer(t *testing.T) {
	input := "$.steps.read_kubeconfig.output[\"success\"].credentials"
	tokenizer := expressions.InitTokenizer(input, "tokenizer_test.go")
	expected_value := []string{"$", ".", "steps", ".", "read_kubeconfig", ".", "output",
		"[", "\"success\"", "]", ".", "credentials"}
	for _, expected := range expected_value {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		next_token, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, next_token.Value, expected)
	}
}

func TestTokenizerWithEscapedStr(t *testing.T) {
	input := `$.output["ab\"|cd"]`
	tokenizer := expressions.InitTokenizer(input, "tokenizer_test.go")
	expected_value := []string{"$", ".", "output", "[", `"ab\"|cd"`, "]"}
	for _, expected := range expected_value {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		next_token, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, next_token.Value, expected)
	}
}

func TestWithFilterType(t *testing.T) {
	input := "$.steps.foo.outputs[\"bar\"][?(@._type=='x')].a"
	tokenizer := expressions.InitTokenizer(input, "tokenizer_test.go")
	expected_value := []string{"$", ".", "steps", ".", "foo", ".", "outputs",
		"[", "\"bar\"", "]", "[", "?", "(", "@", ".", "_type", "=", "=", "'x'", ")", "]", ".", "a"}
	for _, expected := range expected_value {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		next_token, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, next_token.Value, expected)
	}
}

func TestInvalidToken(t *testing.T) {
	input := "[&"
	tokenizer := expressions.InitTokenizer(input, "tokenizer_test.go")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err := tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.Token_id, expressions.MapDelimiterStart)
	assert.Equals(t, tokenVal.Value, "[")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err = tokenizer.GetNext()
	assert.NotNil(t, err)
	assert.Equals(t, tokenVal.Token_id, expressions.UnknownToken)
	assert.Equals(t, tokenVal.Value, "&")
	invalid_token_result, is_correct_err_type := err.(*expressions.InvalidTokenError)
	if !is_correct_err_type {
		t.Fatalf("Error is of incorrect type")
	}
	assert.Equals(t, invalid_token_result.InvalidToken.Column, 2)
	assert.Equals(t, invalid_token_result.InvalidToken.Line, 1)
	assert.Equals(t, invalid_token_result.InvalidToken.Filename, "tokenizer_test.go")
	assert.Equals(t, invalid_token_result.InvalidToken.Value, "&")
}

func TestIntLiteral(t *testing.T) {
	input := "90 09"
	tokenizer := expressions.InitTokenizer(input, "tokenizer_test.go")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err := tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.Token_id, expressions.IntLiteral)
	assert.Equals(t, tokenVal.Value, "90")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	// Numbers that start with 0 appears
	tokenVal, err = tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.Token_id, expressions.IdentifierToken)
	assert.Equals(t, tokenVal.Value, "09")
}

package expressions_test

import (
	"errors"
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/expressions"
)

var filename = "example.go"

func TestTokenizer(t *testing.T) {
	input := `$.steps.read_kubeconfig.output["success"].credentials`
	tokenizer := expressions.InitTokenizer(input, filename)
	expectedValue := []expressions.TokenValue{
		{"$", expressions.RootAccessToken, filename, 1, 1},
		{".", expressions.DotObjectAccessToken, filename, 1, 2},
		{"steps", expressions.IdentifierToken, filename, 1, 3},
		{".", expressions.DotObjectAccessToken, filename, 1, 8},
		{"read_kubeconfig", expressions.IdentifierToken, filename, 1, 9},
		{".", expressions.DotObjectAccessToken, filename, 1, 24},
		{"output", expressions.IdentifierToken, filename, 1, 25},
		{"[", expressions.BracketAccessDelimiterStartToken, filename, 1, 31},
		{"\"success\"", expressions.StringLiteralToken, filename, 1, 32},
		{"]", expressions.BracketAccessDelimiterEndToken, filename, 1, 41},
		{".", expressions.DotObjectAccessToken, filename, 1, 42},
		{"credentials", expressions.IdentifierToken, filename, 1, 43},
	}
	for _, expected := range expectedValue {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		nextToken, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, nextToken.Value, expected.Value)
		assert.Equals(t, nextToken.TokenID, expected.TokenID)
		assert.Equals(t, nextToken.Filename, expected.Filename)
		assert.Equals(t, nextToken.Line, expected.Line)
		assert.Equals(t, nextToken.Column, expected.Column)
	}
}

func TestTokenizerWithEscapedStr(t *testing.T) {
	input := `$.output["ab\"|cd"]`
	tokenizer := expressions.InitTokenizer(input, filename)
	expectedValue := []string{"$", ".", "output", "[", `"ab\"|cd"`, "]"}
	for _, expected := range expectedValue {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		nextToken, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, nextToken.Value, expected)
	}
}

func TestWithFilterType(t *testing.T) {
	input := "$.steps.foo.outputs[\"bar\"][?(@._type=='x')].a"
	tokenizer := expressions.InitTokenizer(input, filename)
	expectedValue := []string{"$", ".", "steps", ".", "foo", ".", "outputs",
		"[", "\"bar\"", "]", "[", "?", "(", "@", ".", "_type", "=", "=", "'x'", ")", "]", ".", "a"}
	for _, expected := range expectedValue {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		nextToken, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, nextToken.Value, expected)
	}
}

func TestInvalidToken(t *testing.T) {
	input := "[&"
	tokenizer := expressions.InitTokenizer(input, filename)
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err := tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.TokenID, expressions.BracketAccessDelimiterStartToken)
	assert.Equals(t, tokenVal.Value, "[")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err = tokenizer.GetNext()
	assert.NotNil(t, err)
	assert.Equals(t, tokenVal.TokenID, expressions.UnknownToken)
	assert.Equals(t, tokenVal.Value, "&")
	expectedError := &expressions.InvalidTokenError{}
	isCorrectErrType := errors.As(err, &expectedError)
	if !isCorrectErrType {
		t.Fatalf("Error is of incorrect type")
	}
	assert.Equals(t, expectedError.InvalidToken.Column, 2)
	assert.Equals(t, expectedError.InvalidToken.Line, 1)
	assert.Equals(t, expectedError.InvalidToken.Filename, filename)
	assert.Equals(t, expectedError.InvalidToken.Value, "&")
}

func TestIntLiteral(t *testing.T) {
	input := "90 09"
	tokenizer := expressions.InitTokenizer(input, filename)
	assert.Equals(t, tokenizer.HasNextToken(), true)
	tokenVal, err := tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.TokenID, expressions.IntLiteralToken)
	assert.Equals(t, tokenVal.Value, "90")
	assert.Equals(t, tokenizer.HasNextToken(), true)
	// Numbers that start with 0 appear to cause error in scanner
	tokenVal, err = tokenizer.GetNext()
	assert.Nil(t, err)
	assert.Equals(t, tokenVal.TokenID, expressions.IdentifierToken)
	assert.Equals(t, tokenVal.Value, "09")
}

func TestWildcard(t *testing.T) {
	input := `$.*`
	tokenizer := expressions.InitTokenizer(input, filename)
	expectedValue := []string{"$", ".", "*"}
	for _, expected := range expectedValue {
		assert.Equals(t, tokenizer.HasNextToken(), true)
		nextToken, err := tokenizer.GetNext()
		assert.NoError(t, err)
		assert.Equals(t, nextToken.Value, expected)
	}
}

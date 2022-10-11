package expand //nolint:testpackage

import (
	"fmt"
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/expressions"
)

var testData = map[string]struct {
	data           any
	expr           string
	error          bool
	expectedResult any
}{
	"root": {
		"Hello world!",
		"$",
		false,
		"Hello world!",
	},
	"sub1": {
		map[string]any{
			"message": "Hello world!",
		},
		"$.message",
		false,
		"Hello world!",
	},
	"sub1map": {
		map[string]any{
			"message": "Hello world!",
		},
		`$["message"]`,
		false,
		"Hello world!",
	},
	"sub2": {
		map[string]any{
			"container": map[string]any{
				"message": "Hello world!",
			},
		},
		"$.container.message",
		false,
		"Hello world!",
	},
	"list": {
		[]string{
			"Hello world!",
		},
		"$[0]",
		false,
		"Hello world!",
	},
}

func TestExpansion(t *testing.T) {
	for name, tc := range testData {
		testCase := tc
		t.Run(name, func(t *testing.T) {
			result, err := expression(
				testCase.expr,
				testCase.data,
			)
			if testCase.error && err == nil {
				t.Fatalf("No error returned")
			}
			if !testCase.error {
				if err != nil {
					t.Fatalf("Unexpected error returned (%v)", err)
				}
				assert.Equals(t, result, testCase.expectedResult)
			}
		})
	}
}

// expression expands a single expression from the baseData passed.
func expression(expression string, baseData any) (any, error) {
	parser, err := expressions.InitParser(expression, "workflow.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression: '%s' (%w)", expression, err)
	}
	ast, err := parser.ParseExpression()
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression: '%s' (%w)", expression, err)
	}
	return evaluate(ast, baseData, baseData)
}

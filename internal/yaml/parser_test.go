// package yaml offers a simplified YAML parser abstraction that not only parses YAML into structures, but also offers
// access to the YAML tags that can be used to assert types. This is used in the workflow engine to distinguish
// expressions from values unambiguously.
package yaml //nolint:testpackage

import (
	"fmt"
	"strings"
	"testing"
)

func assertEqualsYAML(t *testing.T, got Node, expected Node, path ...string) {
	t.Helper()
	if got.Tag() != expected.Tag() {
		if len(path) > 0 {
			t.Fatalf(
				"Mismatching tag at %s, got %s, expected %s",
				strings.Join(path, " -> "),
				got.Tag(),
				expected.Tag(),
			)
		}
		t.Fatalf("Mismatching tag, got %s, expected %s", got.Tag(), expected.Tag())
	}
	if got.Type() != expected.Type() {
		if len(path) > 0 {
			t.Fatalf(
				"Mismatching type at %s, got %s, expected %s",
				strings.Join(path, " -> "),
				got.Type(),
				expected.Type(),
			)
		}
		t.Fatalf("Mismatching type, got %s, expected %s", got.Type(), expected.Type())
	}
	if got.Value() != expected.Value() {
		if len(path) > 0 {
			t.Fatalf(
				"Mismatching value at %s, got %s, expected %s",
				strings.Join(path, " -> "),
				got.Value(),
				expected.Value(),
			)
		}
		t.Fatalf("Mismatching value, got %s, expected %s", got.Value(), expected.Value())
	}
	gotContents := got.Contents()
	expectedContents := expected.Contents()
	if len(gotContents) != len(expectedContents) {
		if len(path) > 0 {
			t.Fatalf(
				"Mismatching content count at %s, got %d, expected %d",
				strings.Join(path, " -> "),
				len(gotContents),
				len(expectedContents),
			)
		}
		t.Fatalf("Mismatching content count, got %d, expected %d", len(gotContents), len(expectedContents))
	}

	for i, gotContent := range gotContents {
		expectedContent := expectedContents[i]
		assertEqualsYAML(t, gotContent, expectedContent, append(path, fmt.Sprintf("%d", i))...)
	}
}

var testData = map[string]struct {
	input          string
	error          bool
	expectedOutput *node
}{
	"simple-key": {
		input: `message: Hello world!`,
		expectedOutput: &node{
			typeID: TypeIDMap,
			tag:    "!!map",
			contents: []Node{
				&node{
					TypeIDString,
					"!!str",
					nil,
					"message",
				},
				&node{
					TypeIDString,
					"!!str",
					nil,
					"Hello world!",
				},
			},
		},
	},
	"simple-key-tag": {
		input: `message: !!test |-
  Hello world!`,
		expectedOutput: &node{
			typeID: TypeIDMap,
			tag:    "!!map",
			contents: []Node{
				&node{
					typeID: TypeIDString,
					tag:    "!!str",
					value:  "message",
				},
				&node{
					typeID: TypeIDString,
					tag:    "!!test",
					value:  "Hello world!",
				},
			},
		},
	},
	"sequence": {
		input: `- test`,
		expectedOutput: &node{
			typeID: TypeIDSequence,
			tag:    "!!seq",
			contents: []Node{
				&node{
					typeID: TypeIDString,
					tag:    "!!str",
					value:  "test",
				},
			},
		},
	},
	"sequence-tags": {
		input: `- !!test |-
  test`,
		expectedOutput: &node{
			typeID: TypeIDSequence,
			tag:    "!!seq",
			contents: []Node{
				&node{
					TypeIDString,
					"!!test",
					nil,
					"test",
				},
			},
		},
	},
	"string": {
		input: `test`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!str",
			value:  "test",
		},
	},
	"int": {
		input: `1`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!int",
			value:  "1",
		},
	},
	"bool": {
		input: `true`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!bool",
			value:  "true",
		},
	},
	"float": {
		input: `1.0`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!float",
			value:  "1.0",
		},
	},
	"null": {
		input: `null`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!null",
			value:  "null",
		},
	},
	"map-int-key": {
		input: `1: test`,
		expectedOutput: &node{
			typeID: TypeIDMap,
			tag:    "!!map",
			contents: []Node{
				&node{
					typeID: TypeIDString,
					tag:    "!!int",
					value:  "1",
				},
				&node{
					typeID: TypeIDString,
					tag:    "!!str",
					value:  "test",
				},
			},
		},
	},
}

func TestYAMLParsing(t *testing.T) {
	t.Parallel()
	for name, tc := range testData {
		testCase := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := New()
			output, err := p.Parse([]byte(testCase.input))
			if err == nil && testCase.error {
				t.Fatalf("No error returned")
			}
			if err != nil && !testCase.error {
				t.Fatalf("Unexpected error returned: %v", err)
			}
			assertEqualsYAML(t, output, testCase.expectedOutput)
		})
	}
}

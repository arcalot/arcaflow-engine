// package yaml offers a simplified YAML parser abstraction that not only parses YAML into structures, but also offers
// access to the YAML tags that can be used to assert types. This is used in the workflow engine to distinguish
// expressions from values unambiguously.
package yaml //nolint:testpackage

import (
	"fmt"
	"strings"
	"testing"

	"go.arcalot.io/assert"
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
	raw            any
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
		raw: map[any]any{"message": "Hello world!"},
	},
	"double-key": {
		input: `message: Hello world!
test: foo`,
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
				&node{
					TypeIDString,
					"!!str",
					nil,
					"test",
				},
				&node{
					TypeIDString,
					"!!str",
					nil,
					"foo",
				},
			},
		},
		raw: map[any]any{"message": "Hello world!", "test": "foo"},
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
		raw: map[any]any{"message": "Hello world!"},
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
		raw: []any{"test"},
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
		raw: []any{"test"},
	},
	"string": {
		input: `test`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!str",
			value:  "test",
		},
		raw: "test",
	},
	"int": {
		input: `1`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!int",
			value:  "1",
		},
		raw: "1",
	},
	"bool": {
		input: `true`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!bool",
			value:  "true",
		},
		raw: "true",
	},
	"float": {
		input: `1.0`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!float",
			value:  "1.0",
		},
		raw: "1.0",
	},
	"null": {
		input: `null`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!null",
			value:  "null",
		},
		raw: "null",
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
		raw: map[any]any{"1": "test"},
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

			if testCase.raw != nil {
				assert.Equals(t, output.Raw(), testCase.raw)
			}
		})
	}
}

func TestMapKey(t *testing.T) {
	parsed, err := New().Parse([]byte(`foo: test
bar: test2
`))
	assert.NoError(t, err)
	n, ok := parsed.MapKey("bar")
	assert.Equals(t, ok, true)
	assert.Equals(t, n.Raw(), "test2")

	_, ok = parsed.MapKey("nonexistent")
	assert.Equals(t, ok, false)
}

func TestMapKeys(t *testing.T) {
	parsed, err := New().Parse([]byte(`foo: test
bar: test2
`))
	assert.NoError(t, err)
	assert.Equals(t, parsed.MapKeys(), []string{"foo", "bar"})
}

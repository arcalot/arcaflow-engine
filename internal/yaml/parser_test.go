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

const emptyYaml = "empty YAML file"

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

const customTag = "!!test"

var helloWorldNode = node{
	TypeIDString,
	"!!str",
	nil,
	"Hello world!",
	nil,
}

var helloWorldCustomTagNode = node{
	TypeIDString,
	customTag,
	nil,
	"Hello world!",
	nil,
}

var fooNode = node{
	TypeIDString,
	"!!str",
	nil,
	"foo",
	nil,
}

const keyMessage = "message"
const keyTest = "test"

var testData = map[string]struct {
	input          string
	error          bool
	errorStr       string
	expectedOutput *node
	raw            any
}{
	//	"simple-key": {
	//		input: `message: Hello world!`,
	//		expectedOutput: &node{
	//			typeID:   TypeIDMap,
	//			tag:      "!!map",
	//			contents: nil,
	//			nodeMap: map[string]Node{
	//				keyMessage: &helloWorldNode,
	//			},
	//		},
	//		raw: map[string]any{"message": "Hello world!"},
	//	},
	//	"double-key": {
	//		input: `message: Hello world!
	//test: foo`,
	//		expectedOutput: &node{
	//			typeID:   TypeIDMap,
	//			tag:      "!!map",
	//			contents: nil,
	//			nodeMap: map[string]Node{
	//				keyMessage: &helloWorldNode,
	//				keyTest:    &fooNode,
	//			},
	//		},
	//		raw: map[string]any{"message": "Hello world!", "test": "foo"},
	//	},
	//	"seq-map": {
	//		input: `- message_1: Hello world 1
	//  message_2: Hello world 2
	//- message_3: Hello world 3
	//		`,
	//		expectedOutput: &node{
	//			typeID: TypeIDSequence,
	//			tag:    "!!seq",
	//			contents: []Node{
	//				node{
	//					TypeIDMap,
	//					"!!map",
	//					[]Node{},
	//					"",
	//					map[string]Node{
	//						"message_1": node{
	//							TypeIDString,
	//							"!!str",
	//							[]Node{},
	//							"Hello world 1",
	//							map[string]Node{},
	//						},
	//						"message_2": node{
	//							TypeIDString,
	//							"!!str",
	//							[]Node{},
	//							"Hello world 2",
	//							map[string]Node{},
	//						},
	//					},
	//				},
	//				node{
	//					TypeIDMap,
	//					"!!map",
	//					[]Node{},
	//					"",
	//					map[string]Node{
	//						"message_3": node{
	//							TypeIDString,
	//							"!!str",
	//							[]Node{},
	//							"Hello world 3",
	//							map[string]Node{},
	//						},
	//					},
	//				},
	//			},
	//			nodeMap: map[string]Node{},
	//		},
	//		raw: []any{
	//			map[string]any{"message_1": "Hello world 1", "message_2": "Hello world 2"},
	//			map[string]any{"message_3": "Hello world 3"},
	//		},
	//	},
	//	"simple-key-tag": {
	//		input: `message: !!test |-
	//Hello world!`,
	//		expectedOutput: &node{
	//			typeID:   TypeIDMap,
	//			tag:      "!!map",
	//			contents: nil,
	//			nodeMap: map[string]Node{
	//				"message": helloWorldCustomTagNode,
	//			},
	//		},
	//		raw: map[string]any{"message": "Hello world!"},
	//	},
	//	"sequence": {
	//		input: `- test`,
	//		expectedOutput: &node{
	//			typeID: TypeIDSequence,
	//			tag:    "!!seq",
	//			contents: []Node{
	//				&node{
	//					typeID:  TypeIDString,
	//					tag:     "!!str",
	//					value:   "test",
	//					nodeMap: map[string]Node{},
	//				},
	//			},
	//		},
	//		raw: []any{"test"},
	//	},
	//	"sequence-tags": {
	//		input: `- !!test |-
	//  test`,
	//		expectedOutput: &node{
	//			typeID: TypeIDSequence,
	//			tag:    "!!seq",
	//			contents: []Node{
	//				&node{
	//					TypeIDString,
	//					"!!test",
	//					nil,
	//					"test",
	//					nil,
	//				},
	//			},
	//		},
	//		raw: []any{"test"},
	//	},
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
	"float_precision-zero": {
		input: `1.`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!float",
			value:  "1.",
		},
		raw: "1.",
	},
	"float_precision-1": {
		input: `1.0`,
		expectedOutput: &node{
			typeID: TypeIDString,
			tag:    "!!float",
			value:  "1.0",
		},
		raw: "1.0",
	},
	"empty-file": {
		input:          ``,
		error:          true,
		errorStr:       emptyYaml,
		expectedOutput: &node{},
		raw:            nil,
	},
	"null-file": {
		input:          `null`,
		error:          true,
		errorStr:       emptyYaml,
		expectedOutput: &node{},
		raw:            nil,
	},
	"empty-map": {
		input: `{}`,
		error: false,
		expectedOutput: &node{
			typeID: TypeIDMap,
			tag:    "!!map",
			value:  "",
		},
		raw: map[string]any{},
	},
	"empty-seq": {
		input: `[]`,
		error: false,
		expectedOutput: &node{
			typeID:   TypeIDSequence,
			tag:      "!!seq",
			value:    "",
			nodeMap:  map[string]Node{},
			contents: []Node{},
		},
		raw: []any{},
	},
	"map-int-key": {
		input: `1: test`,
		error: false,
		expectedOutput: &node{
			typeID:   TypeIDMap,
			tag:      "!!map",
			contents: []Node{},
			nodeMap: map[string]Node{
				"1": &node{
					typeID:   TypeIDString,
					tag:      "!!str",
					value:    "test",
					contents: []Node{},
					nodeMap:  map[string]Node{},
				},
			},
		},
		raw: map[string]any{"1": "test"},
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
				//t.Fatalf("Unexpected error returned: %v", err)
				assert.Contains(t, err.Error(), testCase.errorStr)
			}
			assertEqualsYAML(t, output, testCase.expectedOutput)

			if testCase.raw != nil {
				rawOut := output.Raw()
				assert.Equals(t, rawOut, testCase.raw)
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
	outputMapKeys := make(map[string]struct{})
	for _, k := range parsed.MapKeys() {
		outputMapKeys[k] = struct{}{}
	}
	expectedOutputMapKeys := []string{"foo", "bar"}
	for _, key := range expectedOutputMapKeys {
		assert.MapContainsKey[string, struct{}](t, key, outputMapKeys)
	}
}

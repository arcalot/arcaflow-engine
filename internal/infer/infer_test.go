package infer_test

import (
	"fmt"
	"testing"

	"go.flow.arcalot.io/engine/internal/infer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

type testEntry struct {
	name               string
	input              any
	expectedOutputType schema.TypeID
	validate           func(t schema.Type) error
}

var testData = []testEntry{
	{
		"string",
		"foo",
		schema.TypeIDString,
		func(t schema.Type) error {
			return nil
		},
	},
	{
		"object",
		map[string]any{
			"foo": "bar",
			"baz": 42,
		},
		schema.TypeIDObject,
		func(t schema.Type) error {
			objectSchema := t.(*schema.ObjectSchema)
			properties := objectSchema.Properties()
			if properties["foo"].TypeID() != schema.TypeIDString {
				return fmt.Errorf("incorrect property type for 'foo': %s", properties["foo"].TypeID())
			}
			if properties["baz"].TypeID() != schema.TypeIDInt {
				return fmt.Errorf("incorrect property type for 'foo': %s", properties["foo"].TypeID())
			}
			return nil
		},
	},
	{
		"slice",
		[]string{"foo"},
		schema.TypeIDList,
		func(t schema.Type) error {
			listType := t.(*schema.ListSchema)
			if listType.Items().TypeID() != schema.TypeIDString {
				return fmt.Errorf("incorrect property type list item: %s", listType.Items().TypeID())
			}
			return nil
		},
	},
}

func TestInfer(t *testing.T) {
	for _, entry := range testData {
		entry := entry
		t.Run(entry.name, func(t *testing.T) {
			inferredType, err := infer.Type(entry.input, nil, nil, nil)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if inferredType.TypeID() != entry.expectedOutputType {
				t.Fatalf("Incorrect type inferred: %s", inferredType.TypeID())
			}
		})
	}

}

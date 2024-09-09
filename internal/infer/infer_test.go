package infer_test

import (
	"fmt"
	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.flow.arcalot.io/expressions"
	"testing"

	"go.flow.arcalot.io/engine/internal/infer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

type testEntry struct {
	name               string
	input              any
	dataModel          schema.Scope
	expectedOutputType schema.TypeID
	validate           func(t schema.Type) error
}

var testOneOf = infer.OneOfExpression{
	Discriminator: "option",
	Options: map[string]any{
		"a": map[string]any{
			"value-1": 1,
		},
		"b": map[string]any{
			"value-2": lang.Must2(expressions.New("$.a")),
		},
	},
	Node: "n/a",
}

var testData = []testEntry{
	{
		"string",
		"foo",
		nil,
		schema.TypeIDString,
		func(_ schema.Type) error {
			return nil
		},
	},
	{
		"object",
		map[string]any{
			"foo": "bar",
			"baz": 42,
		},
		nil,
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
		nil,
		schema.TypeIDList,
		func(t schema.Type) error {
			listType := t.(*schema.ListSchema)
			if listType.Items().TypeID() != schema.TypeIDString {
				return fmt.Errorf("incorrect property type list item: %s", listType.Items().TypeID())
			}
			return nil
		},
	},
	{
		"expression-1",
		lang.Must2(expressions.New("$.a")),
		schema.NewScopeSchema(
			schema.NewObjectSchema("root", map[string]*schema.PropertySchema{
				"a": schema.NewPropertySchema(
					schema.NewStringSchema(nil, nil, nil),
					nil,
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
			}),
		),
		schema.TypeIDString,
		func(_ schema.Type) error {
			return nil
		},
	},
	{
		"oneof-expression",
		&testOneOf,
		schema.NewScopeSchema(
			schema.NewObjectSchema("root", map[string]*schema.PropertySchema{
				"a": schema.NewPropertySchema(
					schema.NewStringSchema(nil, nil, nil),
					nil,
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
			}),
		),
		schema.TypeIDOneOfString,
		func(t schema.Type) error {
			return t.ValidateCompatibility(
				schema.NewOneOfStringSchema[any](
					map[string]schema.Object{
						"a": schema.NewObjectSchema("n/a", map[string]*schema.PropertySchema{
							"value-1": schema.NewPropertySchema(
								schema.NewIntSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						}),
						"b": schema.NewObjectSchema("n/a", map[string]*schema.PropertySchema{
							"value-2": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						}),
					},
					"option",
					false,
				),
			)
		},
	},
}

func TestInfer(t *testing.T) {
	for _, entry := range testData {
		entry := entry
		t.Run(entry.name, func(t *testing.T) {
			inferredType, err := infer.Type(entry.input, entry.dataModel, nil, nil)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if inferredType.TypeID() != entry.expectedOutputType {
				t.Fatalf("Incorrect type inferred: %s", inferredType.TypeID())
			}
			assert.NoError(t, entry.validate(inferredType))
		})
	}

}

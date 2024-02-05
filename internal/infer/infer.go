// Package infer provides the ability to construct a schema inferred from existing data and possibly expressions.
package infer

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"time"

	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// OutputSchema either uses the passed output schema or infers the schema from the data model.
func OutputSchema(data any, outputID string, outputSchema *schema.StepOutputSchema, internalDataModel *schema.ScopeSchema, workflowContext map[string][]byte) (*schema.StepOutputSchema, error) {
	if outputSchema == nil {
		inferredScope, err := Scope(data, internalDataModel, workflowContext)
		if err != nil {
			return nil, fmt.Errorf("unable to infer output schema for %s (%w)", outputID, err)
		}
		return schema.NewStepOutputSchema(
			inferredScope,
			nil,
			outputID == "error",
		), nil
	}
	return outputSchema, nil
}

// Scope will infer a scope from the data.
func Scope(data any, internalDataModel *schema.ScopeSchema, workflowContext map[string][]byte) (schema.Scope, error) {
	dataType, err := Type(data, internalDataModel, workflowContext)
	if err != nil {
		return nil, fmt.Errorf("failed to infer data type (%w)", err)
	}
	switch dataType.TypeID() {
	case schema.TypeIDScope:
		return dataType.(schema.Scope), nil
	case schema.TypeIDObject:
		return schema.NewScopeSchema(
			dataType.(*schema.ObjectSchema),
		), nil
	default:
		return nil, fmt.Errorf(
			"invalid type for output root object: %s (must be an object)",
			dataType.TypeID(),
		)
	}
}

// Type attempts to infer the data model from the data, possibly evaluating expressions.
func Type(data any, internalDataModel *schema.ScopeSchema, workflowContext map[string][]byte) (schema.Type, error) {
	if expression, ok := data.(expressions.Expression); ok {
		expressionType, err := expression.Type(internalDataModel, make(map[string]schema.Function), workflowContext) // TODO
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate type of expression %s (%w)", expression.String(), err)
		}
		return expressionType, nil
	}
	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Map:
		return mapType(v, internalDataModel, workflowContext)
	case reflect.Slice:
		return sliceType(v, internalDataModel, workflowContext)
	case reflect.String:
		return schema.NewStringSchema(nil, nil, nil), nil
	case reflect.Int8:
		return schema.NewIntSchema(schema.PointerTo[int64](math.MinInt8), schema.PointerTo[int64](math.MaxInt8), nil), nil
	case reflect.Int16:
		return schema.NewIntSchema(schema.PointerTo[int64](math.MinInt16), schema.PointerTo[int64](math.MaxInt16), nil), nil
	case reflect.Int32:
		return schema.NewIntSchema(schema.PointerTo[int64](math.MinInt32), schema.PointerTo[int64](math.MaxInt32), nil), nil
	case reflect.Int64:
		return schema.NewIntSchema(schema.PointerTo[int64](math.MinInt64), schema.PointerTo[int64](math.MaxInt64), nil), nil
	case reflect.Int:
		return schema.NewIntSchema(schema.PointerTo[int64](math.MinInt), schema.PointerTo[int64](math.MaxInt), nil), nil
	case reflect.Uint8:
		return schema.NewIntSchema(schema.PointerTo[int64](0), schema.PointerTo[int64](math.MaxUint8), nil), nil
	case reflect.Uint16:
		return schema.NewIntSchema(schema.PointerTo[int64](0), schema.PointerTo[int64](math.MaxUint16), nil), nil
	case reflect.Uint32:
		return schema.NewIntSchema(schema.PointerTo[int64](0), schema.PointerTo[int64](math.MaxUint32), nil), nil
	case reflect.Uint64:
		return schema.NewIntSchema(
			schema.PointerTo[int64](0),
			// The MaxInt64 is not a mistake here, uints have a larger range than the Arca-standard 64-bit ints.
			schema.PointerTo[int64](math.MaxInt64),
			nil), nil
	case reflect.Uint:
		return schema.NewIntSchema(
			schema.PointerTo[int64](0),
			// The MaxInt is not a mistake here, uints have a larger range than the Arca-standard 64-bit ints.
			schema.PointerTo[int64](math.MaxInt),
			nil,
		), nil
	case reflect.Float64:
		return schema.NewFloatSchema(nil, nil, nil), nil
	case reflect.Float32:
		return schema.NewFloatSchema(nil, nil, nil), nil
	case reflect.Bool:
		return schema.NewBoolSchema(), nil
	case reflect.Ptr:
		if _, ok := data.(*regexp.Regexp); ok {
			return schema.NewPatternSchema(), nil
		}
		fallthrough
	default:
		return nil, fmt.Errorf("unsupported type for workflow outputs: %T", data)
	}
}

// mapType infers the type of a map value.
func mapType(
	v reflect.Value,
	internalDataModel *schema.ScopeSchema,
	workflowContext map[string][]byte,
) (schema.Type, error) {
	keyType, err := sliceItemType(v.MapKeys(), internalDataModel, workflowContext)
	if err != nil {
		return nil, fmt.Errorf("failed to infer map key type (%w)", err)
	}
	switch keyType.TypeID() {
	case schema.TypeIDString:
		fallthrough
	case schema.TypeIDStringEnum:
		return objectType(v, internalDataModel, workflowContext)
	case schema.TypeIDInt:
	case schema.TypeIDIntEnum:
	default:
		return nil, fmt.Errorf("unsupported type for map keys: %s", keyType.TypeID())
	}
	var foundType schema.Type
	for _, mapKey := range v.MapKeys() {
		mapValue := v.MapIndex(mapKey)
		mapValueAny := mapValue.Interface()
		valueType, err := Type(mapValueAny, internalDataModel, workflowContext)
		if err != nil {
			return nil, fmt.Errorf("failed to infer type of %d (%w)", mapValueAny, err)
		}
		if foundType == nil {
			foundType = valueType
		} else if foundType.TypeID() != valueType.TypeID() {
			return nil, fmt.Errorf(
				"type mismatch in map type (expected: %s, found: %s)",
				foundType.TypeID(),
				valueType.TypeID(),
			)
		}
	}
	if foundType == nil {
		return schema.NewMapSchema(
			keyType,
			schema.NewStringSchema(nil, schema.PointerTo[int64](0), nil),
			nil,
			schema.PointerTo[int64](0),
		), nil
	}
	return schema.NewMapSchema(
		keyType,
		foundType,
		nil,
		nil,
	), nil
}

func objectType(
	value reflect.Value,
	internalDataModel *schema.ScopeSchema,
	workflowContext map[string][]byte,
) (schema.Type, error) {
	properties := make(map[string]*schema.PropertySchema, value.Len())
	for _, keyValue := range value.MapKeys() {
		propertyType, err := Type(value.MapIndex(keyValue).Interface(), internalDataModel, workflowContext)
		if err != nil {
			return nil, fmt.Errorf("failed to infer property %s type (%w)", keyValue.Interface(), err)
		}
		properties[keyValue.Interface().(string)] = schema.NewPropertySchema(
			propertyType,
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}
	return schema.NewObjectSchema(
		generateRandomObjectID(),
		properties,
	), nil
}

// sliceType tries to infer the type of a slice.
func sliceType(
	v reflect.Value,
	internalDataModel *schema.ScopeSchema,
	workflowContext map[string][]byte,
) (schema.Type, error) {
	values := make([]reflect.Value, v.Len())
	for i := 0; i < v.Len(); i++ {
		values[i] = v.Index(i)
	}
	foundType, err := sliceItemType(values, internalDataModel, workflowContext)
	if err != nil {
		return nil, err
	}
	return schema.NewListSchema(
		foundType,
		nil,
		nil,
	), nil
}

func sliceItemType(values []reflect.Value, internalDataModel *schema.ScopeSchema, workflowContext map[string][]byte) (schema.Type, error) {
	types := make([]schema.Type, len(values))
	var foundType schema.Type
	for i, value := range values {
		var err error
		types[i], err = Type(value.Interface(), internalDataModel, workflowContext)
		if err != nil {
			return nil, fmt.Errorf("failed to infer type for item %d (%w)", i, err)
		}
		if foundType == nil {
			foundType = types[i]
		} else if foundType.TypeID() != types[i].TypeID() {
			return nil, fmt.Errorf("mismatching types in list (expected: %s, found: %s)", foundType.TypeID(), types[i].TypeID())
		}
	}
	if foundType == nil {
		return schema.NewStringSchema(nil, nil, nil), nil
	}
	return foundType, nil
}

var characters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
var objectIDRandom = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
func generateRandomObjectID() string {
	result := make([]rune, 32)
	for i := range result {
		result[i] = characters[objectIDRandom.Intn(len(characters))]
	}
	return string(result)
}

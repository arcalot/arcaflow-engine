package workflow

import (
	"fmt"
	"go.flow.arcalot.io/engine/internal/infer"
	"reflect"

	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// newAnySchemaWithExpressions creates an AnySchema which is a wildcard allowing maps, lists, integers, strings, bools
// and floats. It also allows expression objects.
func newAnySchemaWithExpressions() *anySchemaWithExpressions {
	return &anySchemaWithExpressions{}
}

// anySchemaWithExpressions is a wildcard allowing maps, lists, integers, strings, bools, and floats. It also allows
// expression objects.
type anySchemaWithExpressions struct {
	schema.AnySchema
}

func (a *anySchemaWithExpressions) Unserialize(data any) (any, error) {
	return a.checkAndConvert(data)
}

func (a *anySchemaWithExpressions) Validate(data any) error {
	_, err := a.checkAndConvert(data)
	return err
}

func (a *anySchemaWithExpressions) ValidateCompatibility(dataOrType any) error {
	// If expression, resolve it before calling ValidateCompatibility
	if _, ok := dataOrType.(expressions.Expression); ok {
		// Assume okay
		return nil
	}
	return a.AnySchema.ValidateCompatibility(dataOrType)
}

func (a *anySchemaWithExpressions) Serialize(data any) (any, error) {
	return a.checkAndConvert(data)
}

func (a *anySchemaWithExpressions) checkAndConvert(data any) (any, error) {
	if _, ok := data.(expressions.Expression); ok {
		return data, nil
	}
	if _, ok := data.(infer.OneOfExpression); ok {
		return data, nil
	}
	t := reflect.ValueOf(data)
	switch t.Kind() {
	case reflect.Slice:
		result := make([]any, t.Len())
		for i := 0; i < t.Len(); i++ {
			val, err := a.checkAndConvert(t.Index(i).Interface())
			if err != nil {
				return nil, schema.ConstraintErrorAddPathSegment(err, fmt.Sprintf("[%d]", i))
			}
			result[i] = val
		}
		return result, nil
	case reflect.Map:
		result := make(map[any]any, t.Len())
		for _, k := range t.MapKeys() {
			key, err := a.checkAndConvert(k.Interface())
			if err != nil {
				return nil, schema.ConstraintErrorAddPathSegment(err, fmt.Sprintf("{%v}", k))
			}
			v := t.MapIndex(k)
			value, err := a.checkAndConvert(v.Interface())
			if err != nil {
				return nil, schema.ConstraintErrorAddPathSegment(err, fmt.Sprintf("[%v]", key))
			}
			result[key] = value
		}
		return result, nil
	default:
		return a.AnySchema.Unserialize(data)
	}
}

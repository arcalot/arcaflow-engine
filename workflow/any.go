package workflow

import (
	"fmt"
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
	anySchema schema.AnySchema
}

func (a *anySchemaWithExpressions) ReflectedType() reflect.Type {
	return a.anySchema.ReflectedType()
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
	return a.anySchema.ValidateCompatibility(dataOrType)
}

func (a *anySchemaWithExpressions) Serialize(data any) (any, error) {
	return a.checkAndConvert(data)
}

func (a *anySchemaWithExpressions) ApplyNamespace(objects map[string]*schema.ObjectSchema, namespace string) {
	a.anySchema.ApplyNamespace(objects, namespace)
}

func (a *anySchemaWithExpressions) ValidateReferences() error {
	return a.anySchema.ValidateReferences()
}

func (a *anySchemaWithExpressions) TypeID() schema.TypeID {
	return a.anySchema.TypeID()
}

func (a *anySchemaWithExpressions) checkAndConvert(data any) (any, error) {
	if _, ok := data.(expressions.Expression); ok {
		return data, nil
	}
	t := reflect.ValueOf(data)
	switch t.Kind() {
	case reflect.Int:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.String:
		fallthrough
	case reflect.Bool:
		return a.anySchema.Unserialize(data)
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
		return nil, &schema.ConstraintError{
			Message: fmt.Sprintf("unsupported data type for 'any' type: %T", data),
		}
	}
}

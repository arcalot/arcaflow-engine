package util

import (
	"fmt"
	"go.flow.arcalot.io/pluginsdk/schema"
	"reflect"
)

// NewInvalidSerializationDetectorSchema creates a new test schema type.
func NewInvalidSerializationDetectorSchema() *InvalidSerializationDetectorSchema {
	return &InvalidSerializationDetectorSchema{}
}

// InvalidSerializationDetectorSchema is a testing type that detects double serialization or double unserialization,
// which helps prevent bugs.
// It is backed by a string, which is set to the last operation performed. If the last operation matches the
// current operation, then that indicates double serialization or double unserialization.
type InvalidSerializationDetectorSchema struct{}

func (d InvalidSerializationDetectorSchema) Unserialize(data any) (any, error) {
	// The input is expected to always be a string.
	// If the input is equal to "unserialized", then that means it's being unserialized a second time.
	return d.detectInvalidValue("unserialized", data)
}

func (d InvalidSerializationDetectorSchema) detectInvalidValue(operation string, data any) (any, error) {
	asString, isString := data.(string)
	if !isString {
		return nil, &schema.ConstraintError{
			Message: fmt.Sprintf(
				"unsupported data type for InvalidSerializationDetectorSchema; expected string, got %T",
				data,
			),
		}
	}
	if asString == operation {
		return nil, &schema.ConstraintError{
			Message: fmt.Sprintf("InvalidSerializationDetectorSchema double %s", operation),
		}
	}
	return operation, nil
}

func (d InvalidSerializationDetectorSchema) UnserializeType(data any) (string, error) {
	unserialized, err := d.Unserialize(data)
	if err != nil {
		return "", err
	}
	return unserialized.(string), nil
}

func (d InvalidSerializationDetectorSchema) ValidateCompatibility(_ any) error {
	// Not applicable to this data type
	return nil
}

func (d InvalidSerializationDetectorSchema) Validate(data any) error {
	_, err := d.Serialize(data)
	return err
}

func (d InvalidSerializationDetectorSchema) ValidateType(data string) error {
	return d.Validate(data)
}

func (d InvalidSerializationDetectorSchema) Serialize(data any) (any, error) {
	// The input is expected to always be a string.
	// If the input is equal to "serialized", then that means it's being serialized a second time.
	return d.detectInvalidValue("serialized", data)
}

func (d InvalidSerializationDetectorSchema) SerializeType(data string) (any, error) {
	return d.Serialize(data)
}

func (d InvalidSerializationDetectorSchema) ApplyScope(scope schema.Scope) {}

func (d InvalidSerializationDetectorSchema) TypeID() schema.TypeID {
	return schema.TypeIDString // This is a subset of a string schema.
}

func (d InvalidSerializationDetectorSchema) ReflectedType() reflect.Type {
	return reflect.TypeOf("")
}

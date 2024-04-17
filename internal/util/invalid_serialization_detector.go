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

// InvalidSerializationDetectorSchema is a testing type that detects double-
// serialization or double-unserialization which could result in corrupted data.
// The serialization and unserialization methods detect when the operation is
// performed twice in a row on a single piece of data by examining the input
// value; they also count the number of overall operations so that we know that
// the data has been serialized or unserialized at least once (since, if it is
// never operated on, then it's trivial to claim that it was never doubly done).
type InvalidSerializationDetectorSchema struct {
	SerializeCnt   int
	UnserializeCnt int
}

// Unserialize unserializes the data. In this schema that means checking for an
// invalid state and returning "unserialized".
func (d *InvalidSerializationDetectorSchema) Unserialize(data any) (any, error) {
	// The input is expected to always be a string.
	d.UnserializeCnt++
	return d.detectInvalidValue("unserialized", data)
}

func (d *InvalidSerializationDetectorSchema) detectInvalidValue(operation string, data any) (any, error) {
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

// UnserializeType is a string-output-typed version of Unserialize.
func (d *InvalidSerializationDetectorSchema) UnserializeType(data any) (string, error) {
	unserialized, err := d.Unserialize(data)
	if err != nil {
		return "", err
	}
	return unserialized.(string), nil
}

// ValidateCompatibility ensures that the input data or schema is compatible with
// the given InvalidSerializationDetectorSchema.
func (d *InvalidSerializationDetectorSchema) ValidateCompatibility(_ any) error {
	// For convenience, always return "success".
	return nil
}

// Validate ensures that the data can be serialized.
func (d *InvalidSerializationDetectorSchema) Validate(data any) error {
	_, err := d.Serialize(data)
	return err
}

// ValidateType is a string-input typed version of Validate.
func (d *InvalidSerializationDetectorSchema) ValidateType(data string) error {
	return d.Validate(data)
}

// Serialize serializes the data. In this schema that means checking for an
// invalid state, and returning "serialized".
func (d *InvalidSerializationDetectorSchema) Serialize(data any) (any, error) {
	// The input is expected to always be a string.
	d.SerializeCnt++
	return d.detectInvalidValue("serialized", data)
}

// SerializeType is string-input-typed version of Serialize.
func (d *InvalidSerializationDetectorSchema) SerializeType(data string) (any, error) {
	return d.Serialize(data)
}

// ApplyScope is for applying a scope to the references. Does not apply to this object.
func (d *InvalidSerializationDetectorSchema) ApplyScope(_ schema.Scope, _ string) {}

// ValidateReferences validates that all necessary references from scopes have been applied.
// Does not apply to this object.
func (d *InvalidSerializationDetectorSchema) ValidateReferences() error {
	return nil
}

// TypeID returns the category of type this type is. Returns string because
// the valid states of this type include all strings.
func (d *InvalidSerializationDetectorSchema) TypeID() schema.TypeID {
	return schema.TypeIDString // This is a subset of a string schema.
}

// ReflectedType returns the reflect.Type for a string.
func (d *InvalidSerializationDetectorSchema) ReflectedType() reflect.Type {
	return reflect.TypeOf("")
}

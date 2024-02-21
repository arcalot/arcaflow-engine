package util_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/util"
	"go.flow.arcalot.io/pluginsdk/schema"
	"reflect"
	"testing"
)

func TestInvalidSerializationDetectorSchema_ProperUsage(t *testing.T) {
	schemaType := util.NewInvalidSerializationDetectorSchema()
	unserialized, err := schemaType.Unserialize("original")
	assert.NoError(t, err)
	assert.Equals[any](t, unserialized, "unserialized")
	serialized, err := schemaType.Serialize(unserialized)
	assert.NoError(t, err)
	assert.Equals[any](t, serialized, "serialized")
	unserialized, err = schemaType.Unserialize(serialized)
	assert.NoError(t, err)
	assert.Equals[any](t, unserialized, "unserialized")
}

func TestInvalidSerializationDetectorSchema_InvalidInput(t *testing.T) {
	schemaType := util.NewInvalidSerializationDetectorSchema()
	_, err := schemaType.Unserialize(true)
	assert.Error(t, err)
	_, err = schemaType.Serialize(true)
	assert.Error(t, err)
}

func TestInvalidSerializationDetectorSchema_DoubleSerialization(t *testing.T) {
	schemaType := util.NewInvalidSerializationDetectorSchema()
	serialized, err := schemaType.Serialize("original")
	assert.NoError(t, err)
	_, err = schemaType.Serialize(serialized)
	assert.Error(t, err)
	assert.InstanceOf[string](t, serialized)
	_, err = schemaType.SerializeType(serialized.(string))
	assert.Error(t, err)
}

func TestInvalidSerializationDetectorSchema_DoubleUnserialization(t *testing.T) {
	schemaType := util.NewInvalidSerializationDetectorSchema()
	unserialized, err := schemaType.Unserialize("original")
	assert.NoError(t, err)
	_, err = schemaType.Unserialize(unserialized)
	assert.Error(t, err)
	_, err = schemaType.UnserializeType(unserialized)
	assert.Error(t, err)
}

func TestInvalidSerializationDetectorSchema_Other(t *testing.T) {
	schemaType := util.NewInvalidSerializationDetectorSchema()
	// Test TypeID()
	assert.Equals(t, schemaType.TypeID(), schema.TypeIDString)
	// Test Validate and ValidateType
	assert.NoError(t, schemaType.Validate("any string"))
	assert.NoError(t, schemaType.ValidateType("any string"))
	// Test ReflectedType
	assert.Equals(t, schemaType.ReflectedType().Kind(), reflect.String)
	// Cover ValidateCompatibility
	assert.NoError(t, schemaType.ValidateCompatibility(nil))
}

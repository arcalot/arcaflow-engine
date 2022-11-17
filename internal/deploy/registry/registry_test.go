package registry_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.arcalot.io/log"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
)

func TestRegistry_Schema(t *testing.T) {
	t.Parallel()

	t.Run("correct-input", testRegistrySchemaCorrectInput)
	t.Run("incorrect-input", testRegistrySchemaIncorrectInput)
}

func testRegistrySchemaIncorrectInput(t *testing.T) {
	r := registry.New(
		&testNewFactory{},
	)
	schema := r.Schema()

	if _, err := schema.Unserialize(map[string]any{"type": "non-existent"}); err == nil {
		t.Fatalf("No error returned")
	}

	if _, err := schema.Unserialize(map[string]any{}); err == nil {
		t.Fatalf("No error returned")
	}
}

func testRegistrySchemaCorrectInput(t *testing.T) {
	r := registry.New(
		&testNewFactory{},
	)
	schema := r.Schema()

	unserializedData, err := schema.Unserialize(map[string]any{"type": "test"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := unserializedData.(testConfig); !ok {
		t.Fatalf("Incorrect unserialized data type returned: %T", unserializedData)
	}
}

func TestRegistry_Create(t *testing.T) {
	t.Parallel()
	t.Run("correct-creation", testRegistryCreateCorrectCreation)
	t.Run("incorrect-config-type", testRegistryCreateIncorrectConfigType)
	t.Run("nil-config", testRegistryCreateNilConfig)
}

func testRegistryCreateCorrectCreation(t *testing.T) {
	t.Parallel()
	r := registry.New(
		&testNewFactory{},
	)
	connector, err := r.Create(testConfig{}, log.NewTestLogger(t))
	assert.NoError(t, err)
	if _, ok := connector.(*testConnector); !ok {
		t.Fatalf("Incorrect connector returned: %T", connector)
	}
}

func testRegistryCreateIncorrectConfigType(t *testing.T) {
	t.Parallel()
	type testStruct struct {
	}

	r := registry.New(
		&testNewFactory{},
	)
	_, err := r.Create(testStruct{}, log.NewTestLogger(t))
	if err == nil {
		t.Fatalf("expected error, no error returned")
	}
}

func testRegistryCreateNilConfig(t *testing.T) {
	t.Parallel()
	r := registry.New(
		&testNewFactory{},
	)
	_, err := r.Create(nil, log.NewTestLogger(t))
	if err == nil {
		t.Fatalf("expected error, no error returned")
	}
}

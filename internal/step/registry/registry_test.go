package registry_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/step/dummy"
	"go.flow.arcalot.io/engine/internal/step/registry"
)

func TestRegistry(t *testing.T) {
	r, err := registry.New(
		dummy.New(),
	)
	assert.NoError(t, err)

	schema := r.Schema()

	_, err = schema.Unserialize(map[string]any{
		"kind": "dummy",
		"name": "Arca Lot",
	})
	assert.NoError(t, err)

	provider, err := r.GetByKind("dummy")
	assert.NoError(t, err)
	assert.Equals(t, provider.Kind(), "dummy")
}

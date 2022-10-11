package registry_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.arcalot.io/lang"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
	"go.flow.arcalot.io/pluginsdk/schema"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("instantiation", testNewInstantiation)
	t.Run("duplicate-ids", testNewDuplicateIDs)
}

func testNewInstantiation(t *testing.T) {
	t.Parallel()
	r := registry.New(&testNewFactory{})
	factories := r.List()
	assert.Equals(t, len(factories), 1)
	assert.Equals(t, factories["test"].TypeID(), schema.TypeIDScope)
}

func testNewDuplicateIDs(t *testing.T) {
	t.Parallel()

	err := lang.Safe(func() {
		_ = registry.New(
			&testNewFactory{},
			&testNewFactory{},
		)
	})
	if err == nil {
		t.Fatal("No error returned")
	}
}

package workflow

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/schema"
	"testing"
)

var testLifecycleStage = step.LifecycleStageWithSchema{
	LifecycleStage: step.LifecycleStage{},
	InputSchema: map[string]*schema.PropertySchema{
		"scopeA": schema.NewPropertySchema(
			schema.NewScopeSchema(schema.NewObjectSchema("testObjectA", map[string]*schema.PropertySchema{})),
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		),
		"listA": schema.NewPropertySchema(
			schema.NewListSchema(
				schema.NewScopeSchema(
					schema.NewObjectSchema("testObjectB", map[string]*schema.PropertySchema{}),
				),
				nil,
				nil,
			),
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		),
		"intA": schema.NewPropertySchema(
			schema.NewIntSchema(nil, nil, nil),
			nil,
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
		),
	},
	Outputs: map[string]*schema.StepOutputSchema{
		"outputC": schema.NewStepOutputSchema(
			schema.NewScopeSchema(
				schema.NewObjectSchema("testObjectC", map[string]*schema.PropertySchema{}),
			),
			nil,
			false,
		),
	},
}

func TestAddOutputNamespacedScopes(t *testing.T) {
	allNamespaces := make(map[string]schema.Scope)
	expectedPrefix := "TEST_PREFIX_"
	addOutputNamespacedScopes(allNamespaces, testLifecycleStage, expectedPrefix)
	expectedOutput := "outputC"
	expectedNamespace := expectedPrefix + expectedOutput
	assert.Equals(t, len(allNamespaces), 1)
	assert.MapContainsKey(t, expectedNamespace, allNamespaces)
	assert.Equals(t, "testObjectC", allNamespaces[expectedNamespace].Root())
}

func TestAddInputNamespacedScopes(t *testing.T) {
	allNamespaces := make(map[string]schema.Scope)
	expectedPrefix := "TEST_PREFIX_"
	expectedInputs := map[string]string{
		"scopeA": "testObjectA",
		"listA":  "testObjectB",
	}

	addInputNamespacedScopes(allNamespaces, testLifecycleStage, expectedPrefix)
	assert.Equals(t, len(allNamespaces), 2)
	for expectedInput, expectedObject := range expectedInputs {
		expectedNamespace := expectedPrefix + expectedInput
		assert.MapContainsKey(t, expectedNamespace, allNamespaces)
		assert.Equals(t, expectedObject, allNamespaces[expectedNamespace].Root())
	}

}

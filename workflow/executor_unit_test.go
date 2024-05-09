package workflow //nolint:testpackage // Tests private members in the packages

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
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	namespacePrefix := "TEST_PREFIX_"
	addOutputNamespacedScopes(allNamespaces, testLifecycleStage, namespacePrefix)
	expectedNamespace := namespacePrefix + "outputC"
	assert.Equals(t, len(allNamespaces), 1)
	assert.MapContainsKey(t, expectedNamespace, allNamespaces)
	assert.MapContainsKey(t, "testObjectC", allNamespaces[expectedNamespace])
}

func TestAddInputNamespacedScopes(t *testing.T) {
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	namespacePrefix := "TEST_PREFIX_"
	expectedInputs := map[string]string{
		"scopeA": "testObjectA",
		"listA":  "testObjectB",
	}

	addInputNamespacedScopes(allNamespaces, testLifecycleStage, namespacePrefix)
	assert.Equals(t, len(allNamespaces), 2)
	for expectedInput, expectedObject := range expectedInputs {
		expectedNamespace := namespacePrefix + expectedInput
		assert.MapContainsKey(t, expectedNamespace, allNamespaces)
		assert.MapContainsKey(t, expectedObject, allNamespaces[expectedNamespace])
	}
}

func TestAddScopesWithMissingCache(t *testing.T) {
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	externalRef3 := schema.NewNamespacedRefSchema("scopeTestObjectC", "not-applied-namespace", nil)
	notAppliedExternalRefProperty := schema.NewPropertySchema(
		externalRef3,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"notAppliedExternalRef": notAppliedExternalRefProperty,
			},
		),
	)
	assert.PanicsContains(
		t,
		func() {
			addScopesWithReferences(allNamespaces, testScope, "$")
		},
		"scope with namespace \"not-applied-namespace\" was not applied successfully",
	)
}

func TestAddScopesWithReferences(t *testing.T) {
	// Test that the scope itself and the resolved references are added.
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	internalRef := schema.NewRefSchema("scopeTestObjectA", nil)
	internalRefProperty := schema.NewPropertySchema(
		internalRef,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	externalRef1 := schema.NewNamespacedRefSchema("scopeTestObjectB", "test-namespace-1", nil)
	appliedExternalRefProperty := schema.NewPropertySchema(
		externalRef1,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	externalRef2 := schema.NewNamespacedRefSchema("scopeTestObjectB", "$.test-namespace-2", nil)
	rootPrefixAppliedExternalRefProperty := schema.NewPropertySchema(
		externalRef2,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	// This one shouldn't add a namespace.
	nonRefProperty := schema.NewPropertySchema(
		schema.NewStringSchema(nil, nil, nil),
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"internalRef":                  internalRefProperty,
				"appliedExternalRef":           appliedExternalRefProperty,
				"rootPrefixAppliedExternalRef": rootPrefixAppliedExternalRefProperty,
				"nonRefProperty":               nonRefProperty,
			},
		),
	)
	scopeToApply := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectB", map[string]*schema.PropertySchema{},
		),
	)
	testScope.ApplyNamespace(scopeToApply.Objects(), "test-namespace-1")
	testScope.ApplyNamespace(scopeToApply.Objects(), "$.test-namespace-2")
	testScope.ApplySelf()
	addScopesWithReferences(allNamespaces, testScope, "$")
	assert.Equals(t, len(allNamespaces), 3)
	assert.MapContainsKey(t, "$", allNamespaces)
	assert.MapContainsKey(t, "$.appliedExternalRef", allNamespaces)
	assert.MapContainsKey(t, "$.rootPrefixAppliedExternalRef", allNamespaces)
}

func TestApplyAllNamespaces_Pass(t *testing.T) {
	// In this test, we will call applyAllNamespaces and validate that the two namespaces were applied.
	ref1Schema := schema.NewNamespacedRefSchema("scopeTestObjectB", "test-namespace-1", nil)
	ref1Property := schema.NewPropertySchema(
		ref1Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	ref2Schema := schema.NewNamespacedRefSchema("scopeTestObjectC", "test-namespace-2", nil)
	ref2Property := schema.NewPropertySchema(
		ref2Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"ref-b": ref1Property,
				"ref-c": ref2Property,
			},
		),
	)
	// Validate that the refs do not have their object caches yet
	assert.Error(t, testScope.ValidateReferences())
	assert.Panics(t, func() {
		ref1Schema.GetObject()
	})
	assert.Panics(t, func() {
		ref2Schema.GetObject()
	})
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	// Test one manual map creation, and one using scope's `Objects()` method.
	allNamespaces["test-namespace-1"] = map[string]*schema.ObjectSchema{
		"scopeTestObjectB": schema.NewObjectSchema(
			"scopeTestObjectB", map[string]*schema.PropertySchema{},
		),
	}
	allNamespaces["test-namespace-2"] = schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectC", map[string]*schema.PropertySchema{},
		),
	).Objects()
	// Call the function under test and validate that all caches are set.
	assert.NoError(t, applyAllNamespaces(allNamespaces, testScope))
	assert.NoError(t, testScope.ValidateReferences())
	assert.NotNil(t, ref1Schema.GetObject())
	assert.NotNil(t, ref2Schema.GetObject())
}

func TestApplyAllNamespaces_MissingNamespace(t *testing.T) {
	// Test the validation in the applyAllNamespaces function by missing a namespace.
	ref1Schema := schema.NewNamespacedRefSchema("scopeTestObjectB", "test-namespace-1", nil)
	ref1Property := schema.NewPropertySchema(
		ref1Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	ref2Schema := schema.NewNamespacedRefSchema("scopeTestObjectC", "test-namespace-2", nil)
	ref2Property := schema.NewPropertySchema(
		ref2Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"ref-b": ref1Property,
				"ref-c": ref2Property,
			},
		),
	)
	// Validate that the refs do not have their object caches yet
	assert.Error(t, testScope.ValidateReferences())
	assert.Panics(t, func() {
		ref1Schema.GetObject()
	})
	assert.Panics(t, func() {
		ref2Schema.GetObject()
	})
	// Only add test-namespace-1
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	allNamespaces["test-namespace-1"] = schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectB", map[string]*schema.PropertySchema{},
		),
	).Objects()
	err := applyAllNamespaces(allNamespaces, testScope)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error validating references")
	// Ensure the error message lists the single available object.
	assert.Contains(t, err.Error(), "scopeTestObjectB")
}

func TestApplyAllNamespaces_InvalidObject(t *testing.T) {
	// Call applyAllNamespaces with an object reference that doesn't match
	// the scope applied. This tests that the existing validation works.
	ref1Schema := schema.NewNamespacedRefSchema("scopeTestObjectB-wrong", "test-namespace-1", nil)
	ref1Property := schema.NewPropertySchema(
		ref1Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"ref-b": ref1Property,
			},
		),
	)
	allNamespaces := make(map[string]map[string]*schema.ObjectSchema)
	allNamespaces["test-namespace-1"] = schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectB", map[string]*schema.PropertySchema{},
		),
	).Objects()
	assert.PanicsContains(
		t,
		func() {
			_ = applyAllNamespaces(allNamespaces, testScope)
		},
		"object 'scopeTestObjectB-wrong' not found",
	)
}

func TestApplyLifecycleScopes_Basic(t *testing.T) {
	// This tests applyLifeCycleScopes by calling the function with a simple lifecycle,
	// and validating that all references have their objects cached.
	stepLifecycles := make(map[string]step.Lifecycle[step.LifecycleStageWithSchema])
	stepLifecycles["exampleStepID"] = step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: "exampleStageID",
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: step.LifecycleStage{
					ID: "exampleStageID",
				},
				InputSchema: map[string]*schema.PropertySchema{
					"exampleField": schema.NewPropertySchema(
						schema.NewScopeSchema(
							schema.NewObjectSchema(
								"scopeTestObjectB", map[string]*schema.PropertySchema{},
							),
						),
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
					"success": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"successObject",
								map[string]*schema.PropertySchema{
									"exampleField": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										schema.NewDisplayValue(
											schema.PointerTo("Message"),
											nil,
											nil,
										),
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							),
						),
						ErrorValue: false,
					},
					"error": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"error",
								map[string]*schema.PropertySchema{
									"reason": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										schema.NewDisplayValue(
											schema.PointerTo("Message"),
											nil,
											nil,
										),
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
								},
							),
						),
						ErrorValue: true,
					},
				},
			},
		},
	}
	ref1Schema := schema.NewNamespacedRefSchema(
		"scopeTestObjectB",
		"$.steps.exampleStepID.exampleStageID.inputs.exampleField",
		nil,
	)
	ref1Property := schema.NewPropertySchema(
		ref1Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	ref2Schema := schema.NewNamespacedRefSchema(
		"successObject",
		"$.steps.exampleStepID.exampleStageID.outputs.success",
		nil,
	)
	ref2Property := schema.NewPropertySchema(
		ref2Schema,
		nil,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	testScope := schema.NewScopeSchema(
		schema.NewObjectSchema(
			"scopeTestObjectA",
			map[string]*schema.PropertySchema{
				"ref-b": ref1Property,
				"ref-c": ref2Property,
			},
		),
	)
	assert.NoError(t, applyLifecycleScopes(stepLifecycles, testScope))
	assert.NoError(t, testScope.ValidateReferences())
	assert.NotNil(t, ref1Schema.GetObject())
	assert.NotNil(t, ref2Schema.GetObject())
}

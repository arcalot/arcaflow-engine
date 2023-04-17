// Package registry provides the step registry, joining the step providers together.
package registry

import (
	"encoding/json"
	"fmt"

	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// New creates a new step registry from the specified providers.
func New(providers ...step.Provider) (step.Registry, error) {
	p := make(map[string]step.Provider, len(providers))
	objects := make(map[string]schema.Object, len(providers))
	providerKindValues := make(map[string]*schema.DisplayValue, len(providers))
	for _, provider := range providers {
		providerKindValues[provider.Kind()] = nil
	}
	for _, provider := range providers {
		kind := provider.Kind()
		if _, ok := p[kind]; ok {
			return nil, &ErrDuplicateProviderKind{
				provider.Kind(),
			}
		}

		object := map[string]*schema.PropertySchema{}
		defaultKind, err := json.Marshal(provider.Kind())
		if err != nil {
			return nil, fmt.Errorf("the provider kind is not JSON-marshallable: %s", provider.Kind())
		}
		object["kind"] = schema.NewPropertySchema(
			schema.NewStringEnumSchema(providerKindValues),
			schema.NewDisplayValue(
				schema.PointerTo("Step kind"),
				schema.PointerTo("What kind of step to run."),
				nil,
			),
			true,
			nil,
			nil,
			nil,
			schema.PointerTo(string(defaultKind)),
			nil,
		)
		for name, property := range provider.ProviderSchema() {
			object[name] = property
		}
		for name := range provider.RunProperties() {
			if _, ok := object[name]; ok {
				return nil, fmt.Errorf(
					"bug: the field %s is already defined as a provider filed and cannot be reused for run input",
					name,
				)
			}
			object[name] = schema.NewPropertySchema(
				schema.NewAnySchema(),
				nil,
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
		}
		for _, stage := range provider.Lifecycle().Stages {
			for inputField := range stage.InputFields {
				if _, ok := object[inputField]; ok {
					return nil, fmt.Errorf(
						"bug: the %s step provider has a duplicate field named %s",
						provider.Kind(),
						inputField,
					)
				}
				object[inputField] = schema.NewPropertySchema(
					schema.NewAnySchema(),
					nil,
					false,
					nil,
					nil,
					nil,
					nil,
					nil,
				)
			}
		}

		p[kind] = provider
		objects[kind] = schema.NewObjectSchema(provider.Kind(), object)
	}

	registry := &stepRegistry{
		p,
		objects,
	}
	for _, provider := range registry.providers {
		provider.Register(registry)
	}
	return registry, nil
}

type stepRegistry struct {
	providers       map[string]step.Provider
	providerSchemas map[string]schema.Object
}

func (s stepRegistry) Schema() *schema.OneOfSchema[string] {
	return schema.NewOneOfStringSchema[any](s.providerSchemas, "kind")
}

func (s stepRegistry) SchemaByKind(kind string) (schema.Object, error) {
	if _, err := s.GetByKind(kind); err != nil {
		return nil, err
	}
	return s.providerSchemas[kind], nil
}

func (s stepRegistry) GetByKind(kind string) (step.Provider, error) {
	provider, ok := s.providers[kind]
	if !ok {
		kinds := make([]string, len(s.providers))
		i := 0
		for k := range s.providers {
			kinds[i] = k
			i++
		}

		return nil, &step.ErrProviderNotFound{
			Kind:       kind,
			ValidKinds: kinds,
		}
	}
	return provider, nil
}

func (s stepRegistry) List() map[string]step.Provider {
	return s.providers
}

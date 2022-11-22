package step

import "go.flow.arcalot.io/pluginsdk/schema"

// Registry holds the providers for possible steps in workflows.
type Registry interface {
	// Schema provides a generic schema for all steps.
	Schema() *schema.OneOfSchema[string]
	// SchemaByKind returns the schema of a single provider.
	SchemaByKind(kind string) (schema.Object, error)
	// GetByKind returns a provider by its kind value, or
	GetByKind(kind string) (Provider, error)
	// List returns a map of all step providers mapped by their kind values.
	List() map[string]Provider
}

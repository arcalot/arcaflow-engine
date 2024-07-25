package util

import "go.flow.arcalot.io/pluginsdk/schema"

// BuildNamespaceString creates a human-readable string representation of the
// namespace scoped objects.
func BuildNamespaceString(allNamespaces map[string]map[string]*schema.ObjectSchema) string {
	availableObjects := ""
	for namespace, objects := range allNamespaces {
		availableObjects += "\n\t" + namespace + ":"
		for objectID := range objects {
			availableObjects += " " + objectID
		}
	}
	availableObjects += "\n"
	return availableObjects
}

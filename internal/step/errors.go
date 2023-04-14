package step

import (
	"fmt"
	"strings"
)

// ErrProviderNotFound is an error indicating that a provider kind was not found.
type ErrProviderNotFound struct {
	Kind       string
	ValidKinds []string
}

// Error returns the error message.
func (e ErrProviderNotFound) Error() string {
	return fmt.Sprintf(
		"the following step provider is not supported: %s (only the following providers are supported: %s)",
		e.Kind,
		strings.Join(e.ValidKinds, ", "),
	)
}

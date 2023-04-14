package registry

import "fmt"

// ErrDuplicateProviderKind indicates that there are two providers with the same kind value.
type ErrDuplicateProviderKind struct {
	Kind string
}

// Error returns the error message.
func (e ErrDuplicateProviderKind) Error() string {
	return fmt.Sprintf("duplicate provider for kind value %s found", e.Kind)
}

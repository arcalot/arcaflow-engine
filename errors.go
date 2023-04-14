package engine

import "fmt"

// ErrNoWorkflowFile signals that no workflow file was provided in the context.
var ErrNoWorkflowFile = fmt.Errorf("no workflow file provided in context")

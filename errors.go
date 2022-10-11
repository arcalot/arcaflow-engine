package engine

import "fmt"

// ErrWorkflowAborted indicates that the workflow execution was intentionally aborted.
var ErrWorkflowAborted = fmt.Errorf("workflow execution aborted")

// ErrNoWorkflowFile signals that no workflow file was provided in the context.
var ErrNoWorkflowFile = fmt.Errorf("no workflow file provided in context")

// ErrEmptyWorkflowFile signals that the workflow file was provided, but it was empty.
var ErrEmptyWorkflowFile = fmt.Errorf("empty workflow file provided in context")

// ErrNoSteps signals that the workflow has no steps defined.
var ErrNoSteps = fmt.Errorf("no steps defined in workflow")

// ErrInvalidWorkflowYAML signals an invalid YAML in the workflow file.
type ErrInvalidWorkflowYAML struct {
	Cause error
}

func (e ErrInvalidWorkflowYAML) Error() string {
	return fmt.Sprintf("invalid workflow YAML (%v)", e.Cause)
}

func (e ErrInvalidWorkflowYAML) Unwrap() error {
	return e.Cause
}

// ErrInvalidWorkflow indicates that the workflow structure was invalid.
type ErrInvalidWorkflow struct {
	Cause error
}

func (e ErrInvalidWorkflow) Error() string {
	return fmt.Sprintf("invalid workflow (%v)", e.Cause)
}

func (e ErrInvalidWorkflow) Unwrap() error {
	return e.Cause
}

// ErrInvalidInputYAML indicates that the input YAML is syntactically invalid.
type ErrInvalidInputYAML struct {
	Cause error
}

func (e ErrInvalidInputYAML) Error() string {
	return fmt.Sprintf("invalid input YAML (%v)", e.Cause)
}

func (e ErrInvalidInputYAML) Unwrap() error {
	return e.Cause
}

// ErrInvalidInput indicates that the input data is invalid because it does not match the declared schema.
type ErrInvalidInput struct {
	Cause error
}

func (e ErrInvalidInput) Error() string {
	return fmt.Sprintf("invalid input data (%v)", e.Cause)
}

func (e ErrInvalidInput) Unwrap() error {
	return e.Cause
}

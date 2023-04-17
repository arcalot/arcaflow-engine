package step

import "go.flow.arcalot.io/pluginsdk/schema"

// Provider is the description of an item that fits in a workflow. Its implementation provide the
// basis for workflow execution.
type Provider interface {

	// Register notifies the provider of the step registry it belongs to. This function is called directly after
	// creation.
	Register(registry Registry)

	// Kind returns the identifier that uniquely identifies this provider.
	// e.g. "plugin"
	Kind() string

	// Lifecycle describes the lifecycle of the step implemented by this provider.
	Lifecycle() Lifecycle[LifecycleStage]

	// ProviderSchema provides the basic schema of the provider that needs to be fulfilled in order to load the schema
	// itself. The returned value must provide the names of fields and their partial properties.
	ProviderSchema() map[string]*schema.PropertySchema

	// RunProperties provides the names of properties needed to run the step. The properties are provided as a set to
	// limit conflicts, but must, nevertheless, not conflict any fields in the ProviderSchema or any lifecycle stage.
	RunProperties() map[string]struct{}

	// LoadSchema prompts this provider to load its schema and return a step that can actually be run. The provided
	// inputs are guaranteed to match the schema returned by ProviderSchema.
	LoadSchema(inputs map[string]any, workflowContext map[string][]byte) (RunnableStep, error)
}

// StageChangeHandler is a callback hook for reacting to changes in stages. The calling party will be notified of the
// provider advancing stages with this hook.
type StageChangeHandler interface {
	// OnStageChange is the callback that notifies the handler of the fact that the previous stage has finished with
	// the specified output. It indicates which next stage the provider moved to, and if it is waiting for input to
	// be provided via ProvideStageInput.
	//
	// The previous stage may be nil if the callback is called on the first stage the provider enters. The output of the
	// previous stage may also be nil if the stage did not declare any outputs.
	OnStageChange(
		step RunningStep,
		previousStage *string,
		previousStageOutputID *string,
		previousStageOutput *any,
		newStage string,
		waitingForInput bool,
	)

	// OnStepComplete is called when the step has completed a final stage in its lifecycle and communicates the output.
	// The previous output may be nil if the previous stage did not declare any outputs.
	OnStepComplete(
		step RunningStep,
		previousStage string,
		previousStageOutputID *string,
		previousStageOutput *any,
	)
}

// RunnableStep is a step that already has a schema and can be run.
type RunnableStep interface {
	// Lifecycle describes the lifecycle of this step. The data provided is guaranteed to match the RunSchema.
	Lifecycle(input map[string]any) (Lifecycle[LifecycleStageWithSchema], error)
	// RunSchema provides a schema that describes the properties that are required to run the step provided by this
	// provider. These fields must match the RunProperties from the Provider.
	RunSchema() map[string]*schema.PropertySchema
	// Start begins the step execution. This does not necessarily mean that any action is happening, but the step
	// lifecycle will begin and enter its first stage, possibly waiting for input. The provided input is guaranteed to
	// match the RunSchema.
	Start(
		input map[string]any,
		stageChangeHandler StageChangeHandler,
	) (RunningStep, error)
}

// RunningStepState is the state any running step can be in.
type RunningStepState string

const (
	// RunningStepStateStarting indicates that the step hasn't processed its first stage yet.
	RunningStepStateStarting RunningStepState = "starting"
	// RunningStepStateWaitingForInput indicates that the step is currently blocked because it is missing input.
	RunningStepStateWaitingForInput RunningStepState = "waiting_for_input"
	// RunningStepStateRunning indicates that the step is working.
	RunningStepStateRunning RunningStepState = "running"
	// RunningStepStateFinished indicates that the step has finished.
	RunningStepStateFinished RunningStepState = "finished"
)

// RunningStep is the representation of a step that is currently executing.
type RunningStep interface {
	// ProvideStageInput gives you the opportunity to provide input for a stage so that it may continue.
	// The ProvideStageInput must ensure that it only returns once the provider has transitioned to the next
	// stage based on the input, otherwise race conditions may happen.
	ProvideStageInput(stage string, input map[string]any) error
	// CurrentStage returns the stage the step provider is currently in, no matter if it is finished or not.
	CurrentStage() string
	// State returns information about the current step.
	State() RunningStepState
	// Close shuts down the step and cleans up the resources associated with the step.
	Close() error
}

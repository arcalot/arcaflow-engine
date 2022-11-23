package step

import "go.flow.arcalot.io/pluginsdk/schema"

// Provider is the description of an item that fits in a workflow. Its implementation provide the
// basis for workflow execution.
type Provider[ProviderInputType any, RunInputType any] interface {
	// Kind returns the identifier that uniquely identifies this provider.
	// e.g. "plugin"
	Kind() string

	// Lifecycle describes the lifecycle of the step implemented by this provider.
	Lifecycle() Lifecycle[LifecycleStage]

	// PreLoadSchema provides the basic schema of the provider that needs to be fulfilled in order to load the schema
	// itself. It can only be an object because the properties will be merged.
	PreLoadSchema() *schema.TypedObjectSchema[ProviderInputType]

	// LoadSchema prompts this provider to load its schema and return a step that can actually be run.
	LoadSchema(
		input ProviderInputType,
	) (RunnableStep[RunInputType], error)
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
		stage string,
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
type RunnableStep[RunInputType any] interface {
	// RunSchema provides a schema that describes the properties that are required to run the step provided by this
	// provider. It can only be an object because the properties will be merged with the pre-load schema above, and the
	// step stage inputs later.
	RunSchema() *schema.TypedObjectSchema[RunInputType]
	// Start begins the step execution. This does not necessarily mean that any action is happening, but the step
	// lifecycle will begin and enter its first stage, possibly waiting for input.
	Start(
		input RunInputType,
		stageChangeHandler StageChangeHandler,
	) (RunningStep, error)
}

// RunningStep is the representation of a step that is currently executing.
type RunningStep interface {
	// Lifecycle describes the lifecycle of this step.
	Lifecycle() Lifecycle[LifecycleStageWithSchema]
	// ProvideStageInput gives the opportunity to provide input for a stage so that it may continue.
	ProvideStageInput(stage string, input any) error
	// CurrentStage returns the current stage.
	CurrentStage() string
	// Close shuts down the step and cleans up the resources associated with the step.
	Close() error
}

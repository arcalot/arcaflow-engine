package foreach

import (
	"context"
	"fmt"
	"go.arcalot.io/dgraph"
	"reflect"
	"sync"
	"sync/atomic"

	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// New creates a new loop provider.
func New(
	logger log.Logger,
	yamlParserFactory func() (workflow.YAMLConverter, error),
	executorFactory func(logger log.Logger) (workflow.Executor, error),
) (step.Provider, error) {
	return &forEachProvider{
		logger:            logger.WithLabel("source", "foreach-provider"),
		yamlParserFactory: yamlParserFactory,
		executorFactory:   executorFactory,
	}, nil
}

// StageID contains the identifiers for the stages.
type StageID string

const (
	// StageIDExecute is executing the subworkflow.
	StageIDExecute StageID = "execute"
	// StageIDOutputs is providing the output data of the subworkflow.
	StageIDOutputs StageID = "outputs"
	// StageIDFailed is providing the error reason from the subworkflow.
	StageIDFailed StageID = "failed"
	// StageIDEnabling is a stage that indicates that the step is waiting to be enabled.
	// This is required to be separate to ensure that it exits immediately if disabled.
	StageIDEnabling StageID = "enabling"
	// StageIDDisabled is indicating that the step was disabled.
	StageIDDisabled StageID = "disabled"
	// StageIDClosed is a stage that indicates that the workflow has exited or did not start
	// due to workflow termination or step cancellation.
	StageIDClosed StageID = "closed"
)

var executeLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDExecute),
	WaitingName:  "waiting for execution",
	RunningName:  "executing",
	FinishedName: "finished",
	InputFields: map[string]struct{}{
		"items":       {},
		"parallelism": {},
		"wait_for":    {},
	},
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDOutputs): dgraph.AndDependency,
		string(StageIDFailed):  dgraph.CompletionAndDependency,
	},
	Fatal: false,
}
var outputLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDOutputs),
	WaitingName:  "waiting for output",
	RunningName:  "output",
	FinishedName: "output",
	InputFields:  map[string]struct{}{},
	NextStages:   map[string]dgraph.DependencyType{},
	Fatal:        false,
}
var errorLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDFailed),
	WaitingName:  "processing error",
	RunningName:  "processing error",
	FinishedName: "error",
	InputFields:  map[string]struct{}{},
	NextStages:   map[string]dgraph.DependencyType{},
	Fatal:        true,
}
var enablingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDEnabling),
	WaitingName:  "waiting to be enabled",
	RunningName:  "enabling",
	FinishedName: "enablement determined",
	InputFields: map[string]struct{}{
		"enabled": {},
	},
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDExecute):  dgraph.AndDependency,
		string(StageIDDisabled): dgraph.AndDependency,
		string(StageIDClosed):   dgraph.CompletionAndDependency,
	},
}
var disabledLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDisabled),
	WaitingName:  "waiting for the step to be disabled",
	RunningName:  "disabling",
	FinishedName: "disabled",
	InputFields:  map[string]struct{}{},
}
var closedLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDClosed),
	WaitingName:  "closed",
	RunningName:  "closed",
	FinishedName: "closed",
}

type forEachProvider struct {
	logger            log.Logger
	yamlParserFactory func() (workflow.YAMLConverter, error)
	executorFactory   func(logger log.Logger) (workflow.Executor, error)
}

func (l *forEachProvider) Kind() string {
	return "foreach"
}

func (l *forEachProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: "execute",
		Stages: []step.LifecycleStage{
			executeLifecycleStage,
			outputLifecycleStage,
			errorLifecycleStage,
			enablingLifecycleStage,
			disabledLifecycleStage,
			closedLifecycleStage,
		},
	}
}

func (l *forEachProvider) ProviderSchema() map[string]*schema.PropertySchema {
	return map[string]*schema.PropertySchema{
		"workflow": schema.NewPropertySchema(
			schema.NewStringSchema(
				schema.PointerTo[int64](1),
				nil,
				nil,
			),
			schema.NewDisplayValue(
				schema.PointerTo("Workflow file"),
				schema.PointerTo("Workflow file within the workflow context (directory) to loop over."),
				nil,
			),
			true,
			nil,
			nil,
			nil,
			nil,
			[]string{"\"subworkflow.yaml\""},
		),
	}
}

func (l *forEachProvider) RunProperties() map[string]struct{} {
	return map[string]struct{}{}
}

func (l *forEachProvider) LoadSchema(inputs map[string]any, workflowContext map[string][]byte) (step.RunnableStep, error) {
	workflowFileName := inputs["workflow"]
	workflowContents, ok := workflowContext[workflowFileName.(string)]
	if !ok {
		return nil, fmt.Errorf(
			"workflow file %s not found in current workflow context (make sure the subworkflow is in the same directory as the main workflow)",
			workflowFileName.(string),
		)
	}

	yamlConverter, err := l.yamlParserFactory()
	if err != nil {
		return nil, err
	}
	wf, err := yamlConverter.FromYAML(workflowContents)
	if err != nil {
		return nil, err
	}

	executor, err := l.executorFactory(l.logger.WithLabel("subworkflow", workflowFileName.(string)))
	if err != nil {
		return nil, err
	}

	preparedWorkflow, err := executor.Prepare(wf, workflowContext)
	if err != nil {
		return nil, err
	}

	outputSchema := preparedWorkflow.OutputSchema()
	if _, ok := outputSchema["success"]; !ok {
		return nil, fmt.Errorf("the referenced workflow must contain an output named 'success'")
	}

	return &runnableStep{
		workflow: preparedWorkflow,
		logger:   l.logger,
	}, nil
}

type runnableStep struct {
	workflow workflow.ExecutableWorkflow
	logger   log.Logger
}

var parallelismSchema = schema.NewPropertySchema(
	schema.NewIntSchema(
		schema.PointerTo[int64](1),
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Parallelism"),
		schema.PointerTo("How many subworkflows to run in parallel."),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	schema.PointerTo("1"),
	nil,
)

func (r *runnableStep) Lifecycle(_ map[string]any) (step.Lifecycle[step.LifecycleStageWithSchema], error) {
	workflowOutput := r.workflow.OutputSchema()

	return step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: "execute",
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: executeLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					"items": schema.NewPropertySchema(
						schema.NewListSchema(
							r.workflow.Input(),
							nil,
							nil,
						),
						schema.NewDisplayValue(
							schema.PointerTo("Items"),
							schema.PointerTo("Items to loop over."),
							nil,
						),
						true,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
					"wait_for": schema.NewPropertySchema(
						schema.NewAnySchema(),
						schema.NewDisplayValue(
							schema.PointerTo("Wait for condition"),
							schema.PointerTo("Used to wait for a previous step stage to complete before running the step which is waiting."),
							nil,
						),
						false,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
					"parallelism": parallelismSchema,
				},
			},
			{
				LifecycleStage: outputLifecycleStage,
				Outputs: map[string]*schema.StepOutputSchema{
					"success": schema.NewStepOutputSchema(
						schema.NewScopeSchema(
							schema.NewObjectSchema(
								"data",
								map[string]*schema.PropertySchema{
									"data": schema.NewPropertySchema(
										schema.NewListSchema(
											workflowOutput["success"].SchemaValue,
											nil,
											nil,
										),
										schema.NewDisplayValue(
											schema.PointerTo("Data"),
											schema.PointerTo("Data returned from the subworkflows."),
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
						schema.NewDisplayValue(
							schema.PointerTo("Success"),
							schema.PointerTo("Successful results from subworkflows."),
							nil,
						),
						false,
					),
				},
			},
			{
				LifecycleStage: errorLifecycleStage,
				Outputs: map[string]*schema.StepOutputSchema{
					"error": schema.NewStepOutputSchema(
						schema.NewScopeSchema(
							schema.NewObjectSchema(
								"error",
								map[string]*schema.PropertySchema{
									"data": schema.NewPropertySchema(
										schema.NewMapSchema(
											schema.NewIntSchema(nil, nil, nil),
											workflowOutput["success"].SchemaValue,
											nil,
											nil,
										),
										schema.NewDisplayValue(
											schema.PointerTo("Data"),
											schema.PointerTo("Data returned from the subworkflows."),
											nil,
										),
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
									"errors": schema.NewPropertySchema(
										schema.NewMapSchema(
											schema.NewIntSchema(nil, nil, nil),
											schema.NewStringSchema(nil, nil, nil),
											nil,
											nil,
										),
										schema.NewDisplayValue(
											schema.PointerTo("Message"),
											schema.PointerTo("Error message detailing what caused the subworkflow to fail."),
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
						schema.NewDisplayValue(
							schema.PointerTo("Error"),
							schema.PointerTo("Contains the error that happened while executing the subworkflow."),
							nil,
						),
						true,
					),
				},
			},
			{
				LifecycleStage: enablingLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					"enabled": schema.NewPropertySchema(
						schema.NewBoolSchema(),
						schema.NewDisplayValue(
							schema.PointerTo("Enabled"),
							schema.PointerTo("Used to set whether the step is enabled."),
							nil,
						),
						false,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
				},
				Outputs: map[string]*schema.StepOutputSchema{
					"resolved": step.EnabledOutputSchema(),
				},
			},
			{
				LifecycleStage: disabledLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"output": step.DisabledOutputSchema(),
				},
			},
			{
				LifecycleStage: closedLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"result": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"ClosedInfo",
								map[string]*schema.PropertySchema{
									// Unlike a normal step, it cannot be cancelled at this time.
									// That feature can be added later if there is demand.
									"close_requested": schema.NewPropertySchema(
										schema.NewBoolSchema(),
										schema.NewDisplayValue(
											schema.PointerTo("close requested"),
											schema.PointerTo("Whether the step was closed with Close()"),
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
						DisplayValue: nil,
						ErrorValue:   true,
					},
				},
			},
		},
	}, nil
}

func (r *runnableStep) RunSchema() map[string]*schema.PropertySchema {
	return map[string]*schema.PropertySchema{}
}

func (r *runnableStep) Start(_ map[string]any, runID string, stageChangeHandler step.StageChangeHandler) (step.RunningStep, error) {
	ctx, cancel := context.WithCancel(context.Background())
	rs := &runningStep{
		runID:              runID,
		ctx:                ctx,
		cancel:             cancel,
		lock:               &sync.Mutex{},
		currentStage:       StageIDEnabling,
		currentState:       step.RunningStepStateStarting,
		executeInput:       make(chan executeInput, 1),
		enabledInput:       make(chan bool, 1),
		workflow:           r.workflow,
		stageChangeHandler: stageChangeHandler,
		logger:             r.logger,
	}
	go rs.run()
	return rs, nil
}

type executeInput struct {
	data        []any
	parallelism int64
}

type runningStep struct {
	runID                   string
	workflow                workflow.ExecutableWorkflow
	currentStage            StageID
	lock                    *sync.Mutex
	currentState            step.RunningStepState
	executionInputAvailable bool
	executeInput            chan executeInput
	enabledInput            chan bool
	enabledInputAvailable   bool
	ctx                     context.Context
	closed                  atomic.Bool
	wg                      sync.WaitGroup
	cancel                  context.CancelFunc
	stageChangeHandler      step.StageChangeHandler
	logger                  log.Logger
}

func (r *runningStep) ProvideStageInput(stage string, input map[string]any) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed.Load() {
		r.logger.Debugf("exiting foreach ProvideStageInput due to step being closed")
		return nil
	}
	switch stage {
	case string(StageIDExecute):
		items := input["items"]
		v := reflect.ValueOf(items)
		subworkflowInputs := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			item := v.Index(i).Interface()
			_, err := r.workflow.Input().Unserialize(item)
			if err != nil {
				return fmt.Errorf("invalid input item %d for subworkflow (%w) for run/step %s", i, err, r.runID)
			}
			subworkflowInputs[i] = item
		}
		if r.executionInputAvailable {
			return fmt.Errorf("input for execute workflow provided twice for run/step %s", r.runID)
		}
		parallelismInput := input["parallelism"]
		var parallelism int64
		if parallelismInput != nil {
			serializedParallelismInput, err := parallelismSchema.Unserialize(parallelismInput)
			if err != nil {
				return fmt.Errorf("failed to unserialized parallelism input for run/step %s: %w", r.runID, err)
			}
			parallelism = serializedParallelismInput.(int64)
		} else {
			parallelism = int64(1)
		}

		if r.currentState == step.RunningStepStateWaitingForInput && r.currentStage == StageIDExecute {
			r.currentState = step.RunningStepStateRunning
		}
		r.executionInputAvailable = true
		// Send before unlock to ensure that it never gets closed before sending.
		r.executeInput <- executeInput{
			data:        subworkflowInputs,
			parallelism: parallelism,
		}
		return nil
	case string(StageIDOutputs):
		return nil
	case string(StageIDFailed):
		return nil
	case string(StageIDEnabling):
		return r.provideEnablingInput(input)
	case string(StageIDDisabled):
		return nil
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *runningStep) provideEnablingInput(input map[string]any) error {
	// Note: The calling function must have the step mutex locked
	if r.enabledInputAvailable {
		return fmt.Errorf("enabled input provided more than once")
	}
	// Check to make sure it's enabled.
	// This is an optional field, so no input means enabled.
	enabled := input["enabled"] == nil || input["enabled"] == true
	r.enabledInputAvailable = true
	r.enabledInput <- enabled
	return nil
}

func (r *runningStep) CurrentStage() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return string(r.currentStage)
}

func (r *runningStep) State() step.RunningStepState {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.currentState
}

func (r *runningStep) Close() error {
	closedAlready := r.closed.Swap(true)
	if closedAlready {
		r.wg.Wait()
		return nil
	}
	r.cancel()
	r.wg.Wait()
	r.logger.Debugf("Closing inputData channel in foreach step provider")
	close(r.executeInput)
	return nil
}

func (r *runningStep) ForceClose() error {
	// For now, unless it becomes a problem, we'll just call the normal close function.
	return r.Close()
}

func (r *runningStep) run() {
	r.wg.Add(1)
	defer func() {
		r.logger.Debugf("foreach run function done")
		r.wg.Done()
	}()
	waitingForInput := false
	enabled, contextDoneEarly := r.enableStage()
	if contextDoneEarly {
		r.closedEarly(StageIDExecute, true)
		return
	}
	if !enabled {
		r.transitionToDisabled()
		return
	}

	var newState step.RunningStepState
	r.lock.Lock()
	if !r.executionInputAvailable {
		newState = step.RunningStepStateWaitingForInput
		waitingForInput = true
	} else {
		newState = step.RunningStepStateRunning
	}
	r.lock.Unlock()
	enabledOutput := any(map[any]any{"enabled": true})
	// End Enabling with resolved output, and start starting
	r.transitionStageWithOutput(
		StageIDExecute,
		newState,
		schema.PointerTo("resolved"),
		&enabledOutput,
	)
	r.stageChangeHandler.OnStageChange(
		r,
		nil,
		nil,
		nil,
		string(StageIDExecute),
		waitingForInput,
		&r.wg,
	)
	r.runOnInput()
}

// enableStage returns the result of whether the stage was enabled or not.
// Return values:
// - bool: Whether the step was enabled.
// - bool: True if the step was disabled due to context done.
func (r *runningStep) enableStage() (bool, bool) {
	// Enabling is the first stage, so do not transition out of it.
	var enabled bool
	select {
	case enabled = <-r.enabledInput:
	case <-r.ctx.Done():
		return false, true
	}

	if enabled {
		// It's enabled, so the disabled stage will not occur.
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDDisabled), &r.wg, fmt.Errorf("step enabled; cannot be disabled anymore"))
	}
	return enabled, false
}

func (r *runningStep) closedEarly(stageToMarkUnresolvable StageID, priorStageFailed bool) {
	r.logger.Infof("Step foreach %s closed", r.runID)
	// Follow the convention of transitioning to running then finished.
	if priorStageFailed {
		r.transitionFromFailedStage(StageIDClosed, step.RunningStepStateRunning, fmt.Errorf("step closed early"))
	} else {
		r.transitionRunningStage(StageIDClosed)
	}
	closedOutput := any(map[any]any{"close_requested": r.closed.Load()})

	r.completeStep(
		StageIDClosed,
		step.RunningStepStateFinished,
		schema.PointerTo("result"),
		&closedOutput,
	)

	err := fmt.Errorf("step foreach %s closed due to workflow termination", r.runID)
	r.markStageFailures(stageToMarkUnresolvable, err)
}

func (r *runningStep) transitionToDisabled() {
	r.logger.Infof("Step foreach %s disabled", r.runID)
	enabledOutput := any(map[any]any{"enabled": false})
	// End prior stage "enabling" with "resolved" output, and start "disabled" stage.
	r.transitionStageWithOutput(
		StageIDDisabled,
		step.RunningStepStateRunning,
		schema.PointerTo("resolved"),
		&enabledOutput,
	)
	disabledOutput := any(map[any]any{"message": fmt.Sprintf("Step foreach %s disabled", r.runID)})
	r.completeStep(
		StageIDDisabled,
		step.RunningStepStateFinished, // Must set the stage to finished for the engine realize the step is done.
		schema.PointerTo("output"),
		&disabledOutput,
	)

	err := fmt.Errorf("step foreach %s disabled", r.runID)
	r.markStageFailures(StageIDExecute, err)
	r.markNotClosable(err)
}

// Closable is the graceful case, so this is necessary if it crashes.
func (r *runningStep) markNotClosable(err error) {
	r.stageChangeHandler.OnStepStageFailure(r, string(StageIDClosed), &r.wg, err)
}

// TransitionStage transitions the running step to the specified stage, and the state running.
// For other situations, use transitionFromFailedStage, completeStep, or transitionStageWithOutput.
func (r *runningStep) transitionRunningStage(newStage StageID) {
	r.transitionStageWithOutput(newStage, step.RunningStepStateRunning, nil, nil)
}

func (r *runningStep) transitionFromFailedStage(newStage StageID, state step.RunningStepState, err error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = newStage
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.
	r.currentState = state
	r.lock.Unlock()
	r.stageChangeHandler.OnStepStageFailure(
		r,
		previousStage,
		&r.wg,
		err,
	)
}

// TransitionStage transitions the stage to the specified stage, and the state to the specified state.
func (r *runningStep) transitionStageWithOutput(
	newStage StageID,
	state step.RunningStepState,
	outputID *string,
	previousStageOutput *any,
) {
	// A current lack of observability into the atp client prevents
	// non-fragile testing of this function.
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = newStage
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.
	r.currentState = state
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		outputID,
		previousStageOutput,
		string(newStage),
		false,
		&r.wg,
	)
}

//nolint:unparam // Currently only gets state finished, but that's okay.
//nolint:nolintlint // Differing versions of the linter do or do not care.
func (r *runningStep) completeStep(currentStage StageID, state step.RunningStepState, outputID *string, previousStageOutput *any) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = currentStage
	r.currentState = state
	r.lock.Unlock()

	r.stageChangeHandler.OnStepComplete(
		r,
		previousStage,
		outputID,
		previousStageOutput,
		&r.wg,
	)
}

func (r *runningStep) markStageFailures(firstStage StageID, err error) {
	switch firstStage {
	case StageIDEnabling:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDEnabling), &r.wg, err)
		fallthrough
	case StageIDDisabled:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDDisabled), &r.wg, err)
		fallthrough
	case StageIDExecute:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDExecute), &r.wg, err)
		fallthrough
	case StageIDOutputs:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDOutputs), &r.wg, err)
	default:
		panic("unknown StageID " + firstStage)
	}
}

func (r *runningStep) runOnInput() {
	select {
	case loopData, ok := <-r.executeInput:
		if !ok {
			r.logger.Debugf("aborted waiting for result in foreach")
			return
		}
		r.processInput(loopData)
	case <-r.ctx.Done():
		r.logger.Debugf("context done")
		return
	}
}

func (r *runningStep) processInput(input executeInput) {
	r.logger.Debugf("Executing subworkflow for step %s...", r.runID)
	outputs, errors := r.executeSubWorkflows(input)

	r.logger.Debugf("Subworkflow %s complete.", r.runID)
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentState = step.RunningStepStateRunning
	var outputID string
	var outputData any
	var unresolvableStage StageID
	var unresolvableError error
	if len(errors) > 0 {
		r.currentStage = StageIDFailed
		unresolvableStage = StageIDOutputs
		unresolvableError = fmt.Errorf("foreach subworkflow failed with errors (%v)", errors)
		outputID = "error"
		dataMap := make(map[int]any, len(input.data))
		for i, entry := range outputs {
			if entry != nil {
				dataMap[i] = entry
			}
		}
		outputData = map[string]any{
			"data":     dataMap,
			"messages": errors,
		}
	} else {
		r.currentStage = StageIDOutputs
		unresolvableStage = StageIDFailed
		unresolvableError = fmt.Errorf("foreach succeeded, so error case is unresolvable")
		outputID = "success"
		outputData = map[string]any{
			"data": outputs,
		}
	}
	currentStage := r.currentStage
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(currentStage),
		false,
		&r.wg,
	)
	r.stageChangeHandler.OnStepStageFailure(
		r,
		string(unresolvableStage),
		&r.wg,
		unresolvableError,
	)
	r.lock.Lock()
	r.currentState = step.RunningStepStateFinished
	previousStage = string(r.currentStage)
	r.lock.Unlock()
	r.stageChangeHandler.OnStepComplete(r, previousStage, &outputID, &outputData, &r.wg)
}

// returns true if there is an error.
func (r *runningStep) executeSubWorkflows(input executeInput) ([]any, map[int]string) {
	itemOutputs := make([]any, len(input.data))
	itemErrors := make(map[int]string, len(input.data))
	wg := &sync.WaitGroup{}
	wg.Add(len(input.data))
	sem := make(chan struct{}, input.parallelism)
	for i, input := range input.data {
		i := i
		input := input
		go func() {
			defer func() {
				select {
				case <-sem:
				case <-r.ctx.Done(): // Must not deadlock if closed early.
				}
				wg.Done()
			}()
			r.logger.Debugf("Queuing item %d...", i)
			select {
			case sem <- struct{}{}:
			case <-r.ctx.Done():
				r.logger.Debugf("Aborting item %d execution.", i)
				return
			}

			r.logger.Debugf("Executing item %d...", i)
			// Ignore the output ID here because it can only be "success"
			_, outputData, err := r.workflow.Execute(r.ctx, input)
			r.lock.Lock()
			if err != nil {
				itemErrors[i] = err.Error()
			} else {
				itemOutputs[i] = outputData
			}
			r.lock.Unlock()
			r.logger.Debugf("Item %d complete.", i)
		}()
	}
	wg.Wait()
	return itemOutputs, itemErrors
}

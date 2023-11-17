package foreach

import (
	"context"
	"fmt"
	"reflect"
	"sync"

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
)

var executeLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDExecute),
	WaitingName:  "waiting for execution",
	RunningName:  "executing",
	FinishedName: "finished",
	InputFields: map[string]struct{}{
		"items":    {},
		"wait_for": {},
	},
	NextStages: []string{
		string(StageIDOutputs),
		string(StageIDFailed),
	},
	Fatal: false,
}
var outputLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDOutputs),
	WaitingName:  "waiting for output",
	RunningName:  "output",
	FinishedName: "output",
	InputFields:  map[string]struct{}{},
	NextStages:   []string{},
	Fatal:        false,
}
var errorLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDFailed),
	WaitingName:  "processing error",
	RunningName:  "processing error",
	FinishedName: "error",
	InputFields:  map[string]struct{}{},
	NextStages:   []string{},
	Fatal:        true,
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
		"parallelism": schema.NewPropertySchema(
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

	executor, err := l.executorFactory(l.logger)
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
	if len(outputSchema) > 1 {
		return nil, fmt.Errorf("the referenced workflow may only contain a 'success' output")
	}

	return &runnableStep{
		workflow:    preparedWorkflow,
		parallelism: inputs["parallelism"].(int64),
		logger:      l.logger,
	}, nil
}

type runnableStep struct {
	workflow    workflow.ExecutableWorkflow
	parallelism int64
	logger      log.Logger
}

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
		currentStage:       StageIDExecute,
		currentState:       step.RunningStepStateStarting,
		inputData:          make(chan []any, 1),
		workflow:           r.workflow,
		stageChangeHandler: stageChangeHandler,
		parallelism:        r.parallelism,
		logger:             r.logger,
	}
	go rs.run()
	return rs, nil
}

type runningStep struct {
	runID              string
	workflow           workflow.ExecutableWorkflow
	currentStage       StageID
	lock               *sync.Mutex
	currentState       step.RunningStepState
	inputAvailable     bool
	inputData          chan []any
	ctx                context.Context
	wg                 sync.WaitGroup
	cancel             context.CancelFunc
	stageChangeHandler step.StageChangeHandler
	parallelism        int64
	logger             log.Logger
}

func (r *runningStep) ProvideStageInput(stage string, input map[string]any) error {
	r.lock.Lock()
	switch stage {
	case string(StageIDExecute):
		items := input["items"]
		v := reflect.ValueOf(items)
		input := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			item := v.Index(i).Interface()
			_, err := r.workflow.Input().Unserialize(item)
			if err != nil {
				r.lock.Unlock()
				return fmt.Errorf("invalid input item %d for subworkflow (%w) for run/step %s", i, err, r.runID)
			}
			input[i] = item
		}
		if r.inputAvailable {
			r.lock.Unlock()
			return fmt.Errorf("input for execute workflow provided twice for run/step %s", r.runID)
		}
		if r.currentState == step.RunningStepStateWaitingForInput && r.currentStage == StageIDExecute {
			r.currentState = step.RunningStepStateRunning
		}
		r.inputAvailable = true
		r.lock.Unlock()
		r.inputData <- input
		return nil
	case string(StageIDOutputs):
		r.lock.Unlock()
		return nil
	case string(StageIDFailed):
		r.lock.Unlock()
		return nil
	default:
		r.lock.Unlock()
		return fmt.Errorf("invalid stage: %s", stage)
	}
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
	r.cancel()
	r.wg.Wait()
	return nil
}

func (r *runningStep) ForceClose() error {
	// For now, unless it becomes a problem, we'll just call the normal close function.
	return r.Close()
}

func (r *runningStep) run() {
	r.wg.Add(1)
	defer func() {
		close(r.inputData)
		r.wg.Done()
	}()
	waitingForInput := false
	r.lock.Lock()
	if !r.inputAvailable {
		r.currentState = step.RunningStepStateWaitingForInput
		waitingForInput = true
	} else {
		r.currentState = step.RunningStepStateRunning
	}
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		nil,
		nil,
		nil,
		string(StageIDExecute),
		waitingForInput,
		&r.wg,
	)
	select {
	case loopData, ok := <-r.inputData:
		if !ok {
			return
		}

		itemOutputs := make([]any, len(loopData))
		itemErrors := make(map[int]string, len(loopData))

		r.logger.Debugf("Executing subworkflow for step %s...", r.runID)
		wg := &sync.WaitGroup{}
		wg.Add(len(loopData))
		errors := false
		sem := make(chan struct{}, r.parallelism)
		for i, input := range loopData {
			i := i
			input := input
			go func() {
				defer func() {
					<-sem
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
					errors = true
					itemErrors[i] = err.Error()
				} else {
					itemOutputs[i] = outputData
				}
				r.lock.Unlock()
				r.logger.Debugf("Item %d complete.", i)
			}()
		}
		wg.Wait()
		r.logger.Debugf("Subworkflow %s complete.", r.runID)
		r.lock.Lock()
		previousStage := string(r.currentStage)
		r.currentState = step.RunningStepStateRunning
		var outputID string
		var outputData any
		if errors {
			r.currentStage = StageIDFailed
			outputID = "error"
			dataMap := make(map[int]any, len(loopData))
			for i, entry := range itemOutputs {
				if entry != nil {
					dataMap[i] = entry
				}
			}
			outputData = map[string]any{
				"data":     dataMap,
				"messages": itemErrors,
			}
		} else {
			r.currentStage = StageIDOutputs
			outputID = "success"
			outputData = map[string]any{
				"data": itemOutputs,
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
		r.lock.Lock()
		r.currentState = step.RunningStepStateFinished
		previousStage = string(r.currentStage)
		r.lock.Unlock()
		r.stageChangeHandler.OnStepComplete(r, previousStage, &outputID, &outputData, &r.wg)
	case <-r.ctx.Done():
		return
	}

}

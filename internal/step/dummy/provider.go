// Package dummy is a step provider that just says hello. This is intended as a test provider as well as an
// implementation guide for providers.
package dummy

import (
	"context"
	"fmt"
	"sync"

	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// New creates a new dummy provider.
func New() step.Provider {
	return &dummyProvider{}
}

func (p *dummyProvider) Kind() string {
	// This value will uniquely identify the provider.
	return "dummy"
}

type dummyProvider struct {
}

func (p *dummyProvider) ProviderSchema() map[string]*schema.PropertySchema {
	// We don't need any steps to set up the provider.
	return map[string]*schema.PropertySchema{}
}

func (p *dummyProvider) RunProperties() map[string]struct{} {
	// We also don't need any steps to start running the provider.
	return map[string]struct{}{}
}

// StageID is the constant that holds valid plugin stage IDs.
type StageID string

const (
	// StageIDGreet is the demo stage that greets the user.
	StageIDGreet StageID = "greet"
)

var greetingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDGreet),
	WaitingName:  "waiting for greeting",
	RunningName:  "greeting",
	FinishedName: "greeted",
	InputFields: map[string]struct{}{
		"name":     {},
		"nickname": {},
	},
	NextStages: nil,
	Fatal:      false,
}

func (p *dummyProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: string(StageIDGreet),
		Stages: []step.LifecycleStage{
			greetingLifecycleStage,
		},
	}
}

func (p *dummyProvider) LoadSchema(_ map[string]any) (step.RunnableStep, error) {
	return &runnableStep{}, nil
}

type runnableStep struct {
}

func (r *runnableStep) RunSchema() map[string]*schema.PropertySchema {
	return map[string]*schema.PropertySchema{}
}

var inputSchema = map[string]*schema.PropertySchema{
	"name": schema.NewPropertySchema(
		schema.NewStringSchema(schema.PointerTo[int64](1), nil, nil),
		schema.NewDisplayValue(
			schema.PointerTo("Name"),
			nil,
			nil,
		),
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
	),
	"nickname": schema.NewPropertySchema(
		schema.NewStringSchema(schema.PointerTo[int64](1), nil, nil),
		schema.NewDisplayValue(
			schema.PointerTo("Name2"),
			nil,
			nil,
		),
		false,
		nil,
		nil,
		nil,
		nil,
		nil,
	),
}

func (r *runnableStep) Lifecycle(_ map[string]any) (result step.Lifecycle[step.LifecycleStageWithSchema], err error) {
	return step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: string(StageIDGreet),
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: greetingLifecycleStage,
				InputSchema:    inputSchema,
				Outputs: map[string]*schema.StepOutputSchema{
					"success": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"greeting",
								map[string]*schema.PropertySchema{
									"message": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										schema.NewDisplayValue(
											schema.PointerTo("Message"),
											nil,
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
						DisplayValue: schema.NewDisplayValue(
							schema.PointerTo("Success"),
							schema.PointerTo("A nice greeting!"),
							nil,
						),
						ErrorValue: false,
					},
				},
			},
		},
	}, nil
}

func (r *runnableStep) Start(_ map[string]any, stageChangeHandler step.StageChangeHandler) (step.RunningStep, error) {

	ctx, cancel := context.WithCancel(context.Background())

	s := &runningStep{
		stageChangeHandler: stageChangeHandler,
		ctx:                ctx,
		cancel:             cancel,
		lock:               &sync.Mutex{},
		// This name channel will serve as a way to pass the input data when provided. It needs to be buffered so
		// the ProvideInputStage is not blocked.
		name:  make(chan string, 1),
		state: step.RunningStepStateStarting,
	}

	go s.run()

	return s, nil
}

type runningStep struct {
	stageChangeHandler step.StageChangeHandler
	ctx                context.Context
	cancel             context.CancelFunc
	lock               *sync.Mutex
	name               chan string
	state              step.RunningStepState
	inputAvailable     bool
}

func (r *runningStep) State() step.RunningStepState {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.state
}

func (r *runningStep) ProvideStageInput(stage string, input map[string]any) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	switch stage {
	case string(StageIDGreet):
		if r.inputAvailable {
			return fmt.Errorf("bug: input provided more than once")
		}
		obj := schema.NewObjectSchema("input",
			inputSchema,
		)
		inputData, err := obj.Unserialize(input)
		if err != nil {
			return err
		}
		r.inputAvailable = true
		// Make sure the state has a consistent behavior.
		r.state = step.RunningStepStateRunning
		r.name <- inputData.(map[string]any)["name"].(string)
		return nil
	default:
		return fmt.Errorf("bug: invalid stage: %s", stage)
	}
}

func (r *runningStep) CurrentStage() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return string(StageIDGreet)
}

func (r *runningStep) Close() error {
	r.cancel()
	return nil
}

func (r *runningStep) run() {
	defer close(r.name)
	waitingForInput := false
	r.lock.Lock()
	if !r.inputAvailable {
		r.state = step.RunningStepStateWaitingForInput
		waitingForInput = true
	} else {
		r.state = step.RunningStepStateRunning
	}
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		nil,
		nil,
		nil,
		string(StageIDGreet),
		waitingForInput,
	)
	select {
	case name, ok := <-r.name:
		if !ok {
			return
		}
		r.lock.Lock()
		r.state = step.RunningStepStateRunning
		r.lock.Unlock()
		outputID := schema.PointerTo("success")
		outputData := schema.PointerTo[any](map[string]any{
			"message": fmt.Sprintf("Hello %s!", name),
		})
		r.lock.Lock()
		r.state = step.RunningStepStateFinished
		r.lock.Unlock()
		r.stageChangeHandler.OnStepComplete(r, string(StageIDGreet), outputID, outputData)
	case <-r.ctx.Done():
		return
	}
}

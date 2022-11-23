package plugin

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.arcalot.io/log"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/atp"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// New creates a new plugin provider.
func New(logger log.Logger, deployerRegistry registry.Registry, localDeployerConfig any) (step.Provider[SchemaInput, RunInput], error) {
	unserializedLocalDeployerConfig, err := deployerRegistry.Schema().Unserialize(localDeployerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load local deployer configuration, please check your Arcaflow configuration file")
	}
	localDeployer, err := deployerRegistry.Create(unserializedLocalDeployerConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("invalid local deployer configuration, please check your Arcaflow configuration file (%w)", err)
	}
	return &pluginProvider{
		logger:           logger,
		deployerRegistry: deployerRegistry,
		localDeployer:    localDeployer,
	}, nil
}

func (p pluginProvider) Kind() string {
	return "plugin"
}

// SchemaInput is the structure required for the loading schema in a plugin.
type SchemaInput struct {
	Plugin string `json:"plugin"`
}

// RunInput is the structure of information required for the step running.
type RunInput struct {
	Step *string `json:"step"`
}

type pluginProvider struct {
	deployerRegistry registry.Registry
	localDeployer    deployer.Connector
	logger           log.Logger
}

type StageID string

const (
	StageIDDeploy       StageID = "deploy"
	StageIDDeployFailed StageID = "deploy_failed"
	StageIDRunning      StageID = "running"
	StageIDFinished     StageID = "finished"
	StageIDCrashed      StageID = "crashed"
)

var deployingLifecycleStage = step.LifecycleStage{
	ID:              "deploying",
	FinishedKeyword: "deployed",
	InputKeyword:    string(StageIDDeploy),
	NextStages: []string{
		"running", "deploy_failed",
	},
	Fatal: false,
}
var deployFailedLifecycleStage = step.LifecycleStage{
	ID:              string(StageIDDeployFailed),
	FinishedKeyword: "deploy_failed",
	Fatal:           true,
}
var runningLifecycleStage = step.LifecycleStage{
	ID:              string(StageIDRunning),
	FinishedKeyword: "completed",
	NextStages: []string{
		"finished", "crashed",
	},
}
var finishedLifecycleStage = step.LifecycleStage{
	ID:              string(StageIDFinished),
	FinishedKeyword: "finished",
}
var crashedLifecycleStage = step.LifecycleStage{
	ID:              string(StageIDCrashed),
	FinishedKeyword: "crashed",
}

func (p pluginProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: string(StageIDDeploy),
		Stages: []step.LifecycleStage{
			deployingLifecycleStage,
			deployFailedLifecycleStage,
			runningLifecycleStage,
			finishedLifecycleStage,
			crashedLifecycleStage,
		},
	}
}

func (p pluginProvider) PreLoadSchema() *schema.TypedObjectSchema[SchemaInput] {
	return schema.NewTypedObject[SchemaInput](
		"PluginProviderSchemaInput",
		map[string]*schema.PropertySchema{
			"plugin": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Plugin"),
					schema.PointerTo("Container image with plugin to run."),
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
	)
}

func (p pluginProvider) LoadSchema(input SchemaInput) (step.RunnableStep[RunInput], error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	plugin, err := p.localDeployer.Deploy(ctx, input.Plugin)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to deploy plugin from image %s (%w)", input.Plugin, err)
	}
	// Set up the ATP connnection
	transport := atp.NewClientWithLogger(plugin, p.logger)
	// Read the schema information
	s, err := transport.ReadSchema()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to read plugin schema from %s (%w)", input.Plugin, err)
	}
	// Shut down the plugin.
	if err := plugin.Close(); err != nil {
		return nil, fmt.Errorf("failed to shut down local plugin from %s (%w)", input.Plugin, err)
	}

	return &runnableStep{
		schemas:          s,
		logger:           p.logger,
		image:            input.Plugin,
		deployerRegistry: p.deployerRegistry,
	}, nil
}

type runnableStep struct {
	image            string
	deployerRegistry registry.Registry
	logger           log.Logger
	schemas          schema.Schema[schema.Step]
}

func (r runnableStep) RunSchema() *schema.TypedObjectSchema[RunInput] {
	return schema.NewTypedObject[RunInput](
		"PluginProviderRunInput",
		map[string]*schema.PropertySchema{
			"step": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Image"),
					schema.PointerTo("Container image with plugin to run."),
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
	)
}

func (r *runnableStep) Start(input RunInput, stageChangeHandler step.StageChangeHandler) (step.RunningStep, error) {
	steps := r.schemas.Steps()
	if input.Step == nil {
		if len(steps) > 1 {
			stepNames := make([]string, len(steps))
			i := 0
			for stepName := range steps {
				stepNames[i] = stepName
				i++
			}
			return nil, fmt.Errorf(
				"the %s plugin declares more than one possible step, please provide the step name (one of: %s)",
				r.image,
				strings.Join(stepNames, ", "),
			)
		}
		for stepName := range steps {
			stepName := stepName
			input.Step = &stepName
		}
	}
	stepSchema, ok := steps[*input.Step]
	if !ok {
		return nil, fmt.Errorf(
			"plugin %s does not have a step named %s",
			r.image,
			*input.Step,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &runningStep{
		stageChangeHandler: stageChangeHandler,
		stepSchema:         stepSchema,
		deployerRegistry:   r.deployerRegistry,
		currentStage:       StageIDDeploy,
		lock:               &sync.Mutex{},
		ctx:                ctx,
		cancel:             cancel,
		done:               make(chan struct{}),
		deployInput:        make(chan any, 1),
		runInput:           make(chan any, 1),
		logger:             r.logger,
		image:              r.image,
		step:               *input.Step,
	}

	go s.run()

	return s, nil
}

type runningStep struct {
	deployerRegistry     registry.Registry
	stepSchema           schema.Step
	stageChangeHandler   step.StageChangeHandler
	lock                 *sync.Mutex
	ctx                  context.Context
	cancel               context.CancelFunc
	done                 chan struct{}
	deployInput          chan any
	deployInputAvailable bool
	runInput             chan any
	runInputAvailable    bool
	logger               log.Logger
	currentStage         StageID
	image                string
	step                 string
}

//nolint:funlen
func (r runningStep) Lifecycle() step.Lifecycle[step.LifecycleStageWithSchema] {
	return step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: "deploying",
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: deployingLifecycleStage,
				InputSchema:    r.deployerRegistry.Schema(),
				Outputs:        nil,
			},
			{
				LifecycleStage: deployFailedLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"error": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"DeployError",
								map[string]*schema.PropertySchema{
									"error": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
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
			{
				LifecycleStage: runningLifecycleStage,
				InputSchema:    r.stepSchema.Input(),
				Outputs:        r.stepSchema.Outputs(),
			},
			{
				LifecycleStage: finishedLifecycleStage,
				InputSchema:    nil,
				Outputs:        nil,
			},
			{
				LifecycleStage: crashedLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"error": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"Crashed",
								map[string]*schema.PropertySchema{
									"output": schema.NewPropertySchema(
										schema.NewStringSchema(nil, nil, nil),
										nil,
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
	}
}

func (r runningStep) ProvideStageInput(stage string, input any) error {
	r.lock.Lock()

	switch stage {
	case string(StageIDDeploy):
		if r.deployInputAvailable {
			r.lock.Unlock()
			return fmt.Errorf("deployment information provided more than once")
		}
		unserializedDeployerConfig, err := r.deployerRegistry.Schema().Unserialize(input)
		if err != nil {
			r.lock.Unlock()
			return fmt.Errorf("invalid deployment information (%w)", err)
		}
		r.deployInputAvailable = true
		r.lock.Unlock()
		r.deployInput <- unserializedDeployerConfig
		return nil
	case string(StageIDRunning):
		if r.runInputAvailable {
			r.lock.Unlock()
			return fmt.Errorf("input provided more than once")
		}
		if _, err := r.stepSchema.Input().Unserialize(input); err != nil {
			r.lock.Unlock()
			return err
		}
		r.runInputAvailable = true
		r.lock.Unlock()
		r.runInput <- input
		return nil
	default:
		return fmt.Errorf("bug: invalid stage: %s", stage)
	}
}

func (r runningStep) CurrentStage() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return string(r.currentStage)
}

func (r runningStep) Close() error {
	r.cancel()
	<-r.done
	return nil
}

func (r runningStep) run() {
	defer close(r.done)
	container, err := r.deployStage()
	if err != nil {
		r.deployFailed(err)
		return
	}
	defer func() {
		if err := container.Close(); err != nil {
			r.logger.Errorf("failed to remove deployed container (%w)", err)
		}
	}()
	if err := r.runStage(container); err != nil {
		r.runFailed(err)
	}
}

func (r runningStep) deployStage() (deployer.Plugin, error) {
	var deployerConfig any
	select {
	case deployerConfig = <-r.deployInput:
	case <-r.ctx.Done():
		return nil, fmt.Errorf("step closed before deployment config could be obtained")
	}

	stepDeployer, err := r.deployerRegistry.Create(deployerConfig, r.logger)
	if err != nil {
		return nil, err
	}
	container, err := stepDeployer.Deploy(r.ctx, r.image)
	if err != nil {
		return nil, err
	}
	return container, nil
}

func (r runningStep) runStage(container deployer.Plugin) error {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDRunning
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDDeployFailed),
		r.runInputAvailable,
	)
	var runInput any
	select {
	case runInput = <-r.runInput:
	case <-r.ctx.Done():
		return fmt.Errorf("step closed while waiting for run configuration")
	}
	atpClient := atp.NewClientWithLogger(container, r.logger)

	inputSchema, err := atpClient.ReadSchema()
	if err != nil {
		return err
	}
	steps := inputSchema.Steps()
	stepSchema, ok := steps[r.step]
	if !ok {
		return fmt.Errorf("schema mismatch between local and remote deployed plugin, no stepSchema named %s found in remote", r.step)
	}
	if _, err := stepSchema.Input().Unserialize(runInput); err != nil {
		return fmt.Errorf("schema mismatch between local and remote deployed plugin, unserializing input failed (%w)", err)
	}

	outputID, outputData, err := atpClient.Execute(r.ctx, r.step, runInput)
	if err != nil {
		return err
	}

	// Execution complete, move to finished stage.
	r.lock.Lock()
	previousStage = string(r.currentStage)
	r.currentStage = StageIDFinished
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(r.currentStage),
		false)
	r.stageChangeHandler.OnStepComplete(
		r,
		string(r.currentStage),
		&outputID,
		&outputData,
	)
	return nil
}

func (r runningStep) deployFailed(err error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDDeployFailed
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDDeployFailed),
		false)
	outputID := "error"
	output := any(DeployFailed{
		Error: err.Error(),
	})
	r.stageChangeHandler.OnStepComplete(
		r,
		string(r.currentStage),
		&outputID,
		&output,
	)
}

func (r runningStep) runFailed(err error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDCrashed
	r.lock.Unlock()
	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(r.currentStage),
		false)
	outputID := "error"
	output := any(Crashed{
		Output: err.Error(),
	})
	r.stageChangeHandler.OnStepComplete(
		r,
		string(r.currentStage),
		&outputID,
		&output,
	)
}

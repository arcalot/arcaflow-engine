package plugin

import (
	"context"
	"fmt"
	"strings"
	"sync"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/deployer/registry"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/pluginsdk/atp"
	"go.flow.arcalot.io/pluginsdk/schema"
)

const errorStr = "error"

// New creates a new plugin provider.
// deployerRegistry The registry that contains all possible deployers.
// localDeployerConfig The section of the workflow config that pertains to all of the
//
//	deployers. Most importantly it specifies which deployer is used for this
//	deployment with the 'type' key.
//	For more info, see `config/schema.go`
func New(logger log.Logger, deployerRegistry registry.Registry, localDeployerConfig any) (step.Provider, error) {
	unserializedLocalDeployerConfig, err := deployerRegistry.Schema().Unserialize(localDeployerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load local deployer configuration, please check your Arcaflow configuration file (%w)", err)
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

func (p *pluginProvider) Kind() string {
	return "plugin"
}

type pluginProvider struct {
	deployerRegistry registry.Registry
	localDeployer    deployer.Connector
	logger           log.Logger
}

func (p *pluginProvider) Register(_ step.Registry) {
}

func (p *pluginProvider) ProviderSchema() map[string]*schema.PropertySchema {
	return map[string]*schema.PropertySchema{
		"plugin": schema.NewPropertySchema(
			schema.NewStringSchema(schema.PointerTo[int64](1), nil, nil),
			schema.NewDisplayValue(
				schema.PointerTo("Plugin"),
				schema.PointerTo("Plugin container image to run. This image must be an Arcaflow-compatible container."),
				nil,
			),
			true,
			nil,
			nil,
			nil,
			nil,
			[]string{"\"quay.io/arcaflow/example-plugin:latest\""},
		),
	}
}

func (p *pluginProvider) RunProperties() map[string]struct{} {
	return map[string]struct{}{
		"step": {},
	}
}

// StageID is the constant that holds valid plugin stage IDs.
type StageID string

const (
	// StageIDDeploy is the stage when the plugin container gets deployd.
	StageIDDeploy StageID = "deploy"
	// StageIDDeployFailed is the stage after a plugin container deployment failed.
	StageIDDeployFailed StageID = "deploy_failed"
	// StageIDRunning is a stage that indicates that a plugin is now working.
	StageIDRunning StageID = "running"
	// StageIDCancelled is a stage that indicates that the plugin's step was cancelled.
	StageIDCancelled StageID = "cancelled"
	// StageIDOutput is a stage that indicates that the plugin has completed working successfully.
	StageIDOutput StageID = "outputs"
	// StageIDCrashed is a stage that indicates that the plugin has quit unexpectedly.
	StageIDCrashed StageID = "crashed"
	// StageIDStarting is a stage that indicates that the plugin execution has begun.
	StageIDStarting StageID = "starting"
)

var deployingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDeploy),
	WaitingName:  "waiting for deployment",
	RunningName:  "deploying",
	FinishedName: "deployed",
	InputFields:  map[string]struct{}{string(StageIDDeploy): {}},
	NextStages: []string{
		string(StageIDStarting), string(StageIDDeployFailed),
	},
	Fatal: false,
}
var deployFailedLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDeployFailed),
	WaitingName:  "deploy failed",
	RunningName:  "deploy failed",
	FinishedName: "deploy failed",
	Fatal:        true,
}

var startingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDStarting),
	WaitingName:  "waiting to start",
	RunningName:  "starting",
	FinishedName: "started",
	InputFields: map[string]struct{}{
		//nolint:godox
		// TODO: Add wait_for here. Empty struct.
		"input": {},
	},
	NextStages: []string{
		string(StageIDRunning), string(StageIDCrashed),
	},
}

var runningLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDRunning),
	WaitingName:  "waiting to run",
	RunningName:  "running",
	FinishedName: "completed",
	InputFields:  map[string]struct{}{},
	NextStages: []string{
		string(StageIDOutput), string(StageIDCrashed),
	},
}

var cancelledLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDCancelled),
	WaitingName:  "waiting for stop condition",
	RunningName:  "cancelling",
	FinishedName: "cancelled",
	InputFields: map[string]struct{}{
		"stop_if": {},
	},
	NextStages: []string{
		string(StageIDOutput), string(StageIDCrashed), string(StageIDDeployFailed),
	},
}
var finishedLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDOutput),
	WaitingName:  "finished",
	RunningName:  "finished",
	FinishedName: "finished",
}
var crashedLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDCrashed),
	WaitingName:  "crashed",
	RunningName:  "crashed",
	FinishedName: "crashed",
}

func (p *pluginProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: string(StageIDDeploy),
		Stages: []step.LifecycleStage{
			deployingLifecycleStage,
			deployFailedLifecycleStage,
			startingLifecycleStage,
			runningLifecycleStage,
			cancelledLifecycleStage,
			finishedLifecycleStage,
			crashedLifecycleStage,
		},
	}
}

func (p *pluginProvider) LoadSchema(inputs map[string]any, _ map[string][]byte) (step.RunnableStep, error) {
	image := inputs["plugin"].(string)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	plugin, err := p.localDeployer.Deploy(ctx, image)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to deploy plugin from image %s (%w)", image, err)
	}
	// Set up the ATP connection
	transport := atp.NewClientWithLogger(plugin, p.logger)
	// Read the schema information
	s, err := transport.ReadSchema()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to read plugin schema from %s (%w)", image, err)
	}
	// Shut down the plugin.
	if err := plugin.Close(); err != nil {
		return nil, fmt.Errorf("failed to shut down local plugin from %s (%w)", image, err)
	}

	return &runnableStep{
		schemas:          s,
		logger:           p.logger,
		image:            image,
		deployerRegistry: p.deployerRegistry,
		localDeployer:    p.localDeployer,
	}, nil
}

type runnableStep struct {
	image            string
	deployerRegistry registry.Registry
	logger           log.Logger
	schemas          schema.Schema[schema.Step]
	localDeployer    deployer.Connector
}

func (r *runnableStep) RunSchema() map[string]*schema.PropertySchema {
	required := len(r.schemas.Steps()) > 1
	return map[string]*schema.PropertySchema{
		"step": schema.NewPropertySchema(
			schema.NewStringSchema(schema.IntPointer(1), nil, nil),
			schema.NewDisplayValue(
				schema.PointerTo("Step"),
				schema.PointerTo("Step to run."),
				nil,
			),
			required,
			nil,
			nil,
			nil,
			nil,
			nil,
		),
	}
}

func (r *runnableStep) Lifecycle(input map[string]any) (result step.Lifecycle[step.LifecycleStageWithSchema], err error) {
	rawStepID, ok := input["step"]
	if !ok || rawStepID == nil {
		rawStepID = ""
	}
	stepID := rawStepID.(string)

	steps := r.schemas.Steps()
	if stepID == "" {
		if len(steps) != 1 {
			return result, fmt.Errorf("the 'step' parameter is required for the '%s' plugin", r.image)
		}
		for possibleStepID := range steps {
			stepID = possibleStepID
		}
	}
	stepSchema, ok := r.schemas.Steps()[stepID]
	if !ok {
		return result, fmt.Errorf("the step '%s' does not exist in the '%s' plugin", stepID, r.image)
	}
	return step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: "deploying",
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: deployingLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					"deploy": schema.NewPropertySchema(
						r.deployerRegistry.Schema(),
						schema.NewDisplayValue(
							schema.PointerTo("Deployment configuration"),
							schema.PointerTo(
								"Provide the the deployment configuration for this step. If left empty, the local deployer will be used.",
							),
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
				Outputs: nil,
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
									errorStr: schema.NewPropertySchema(
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
				LifecycleStage: startingLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					//nolint:godox
					// TODO: Add wait_for right here. Should be an any type.
					// Also add to section above.
					"input": schema.NewPropertySchema(
						stepSchema.Input(),
						stepSchema.Display(),
						true,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
				},
			},
			{
				LifecycleStage: runningLifecycleStage,
				InputSchema:    nil,
				Outputs:        nil,
			},
			{
				LifecycleStage: cancelledLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					"stop_if": schema.NewPropertySchema(
						schema.NewAnySchema(),
						schema.NewDisplayValue(
							schema.PointerTo("Stop condition"),
							schema.PointerTo("If this field is filled with a non-false value, the step is cancelled (even if currently executing)."),
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
				Outputs: nil,
			},
			{
				LifecycleStage: finishedLifecycleStage,
				InputSchema:    nil,
				Outputs:        stepSchema.Outputs(),
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
	}, nil
}

func (r *runnableStep) Start(input map[string]any, stageChangeHandler step.StageChangeHandler) (step.RunningStep, error) {
	rawStep, ok := input["step"]
	stepID := ""
	if ok && rawStep != nil {
		stepID = rawStep.(string)
	}

	steps := r.schemas.Steps()
	if stepID == "" {
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
			stepID = stepName
		}
	}
	stepSchema, ok := steps[stepID]
	if !ok {
		return nil, fmt.Errorf(
			"plugin %s does not have a step named %s",
			r.image,
			stepID,
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
		step:               stepID,
		state:              step.RunningStepStateStarting,
		localDeployer:      r.localDeployer,
		executionChannel:   make(chan executionResult),
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
	state                step.RunningStepState
	useLocalDeployer     bool
	localDeployer        deployer.Connector
	container            deployer.Plugin
	executionChannel     chan executionResult
}

func (r *runningStep) CurrentStage() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return string(r.currentStage)
}

func (r *runningStep) State() step.RunningStepState {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.state
}

func (r *runningStep) ProvideStageInput(stage string, input map[string]any) error {
	// If you change the running step's Stage in this function it can
	// affect the counting of step states in the workflow's Execute function
	// and notifySteps function.
	r.lock.Lock()

	// Checks which stage it is getting input for
	switch stage {
	case string(StageIDDeploy):
		// input provided on this call overwrites the deployer configuration
		// set at this plugin provider's instantiation
		if r.deployInputAvailable {
			r.lock.Unlock()
			return fmt.Errorf("deployment information provided more than once")
		}
		var unserializedDeployerConfig any
		var err error
		if input["deploy"] != nil {
			unserializedDeployerConfig, err = r.deployerRegistry.Schema().Unserialize(input["deploy"])
			if err != nil {
				r.lock.Unlock()
				return fmt.Errorf("invalid deployment information (%w)", err)
			}
		} else {
			r.useLocalDeployer = true
		}
		// Make sure we transition the state before unlocking so there are no race conditions.

		r.deployInputAvailable = true
		if r.state == step.RunningStepStateWaitingForInput && r.currentStage == StageIDDeploy {
			r.state = step.RunningStepStateRunning
		}
		r.lock.Unlock()
		// Feed the deploy step its input.
		r.deployInput <- unserializedDeployerConfig
		return nil
	case string(StageIDStarting):
		if r.runInputAvailable {
			r.lock.Unlock()
			return fmt.Errorf("input provided more than once")
		}
		if input["input"] == nil {
			r.lock.Unlock()
			return fmt.Errorf("bug: invalid input for 'running' stage, expected 'input' field")
		}
		if _, err := r.stepSchema.Input().Unserialize(input["input"]); err != nil {
			r.lock.Unlock()
			return err
		}
		// Make sure we transition the state before unlocking so there are no race conditions.
		r.runInputAvailable = true
		if r.state == step.RunningStepStateWaitingForInput && r.currentStage == StageIDStarting {
			r.state = step.RunningStepStateRunning
		}
		// Unlock before passing the data over the channel to prevent a deadlock.
		// The other end of the channel needs to be unlocked to read the data.
		r.lock.Unlock()
		// Feed the run step its input over the channel.
		r.runInput <- input["input"]
		return nil
	case string(StageIDRunning):
		r.lock.Unlock()
		return nil
	case string(StageIDCancelled):
		if input["stop_if"] != false && input["stop_if"] != nil {
			r.logger.Infof("Cancelling step %s", r.step)
			r.cancel() // This should cancel the plugin deployment or execution.
		}
		r.lock.Unlock()
		return nil
	case string(StageIDDeployFailed):
		r.lock.Unlock()
		return nil
	case string(StageIDCrashed):
		r.lock.Unlock()
		return nil
	case string(StageIDOutput):
		r.lock.Unlock()
		return nil
	default:
		r.lock.Unlock()
		return fmt.Errorf("bug: invalid stage: %s", stage)
	}
}

func (r *runningStep) Close() error {
	r.cancel()
	r.lock.Lock()
	if r.container != nil {
		if err := r.container.Close(); err != nil {
			return fmt.Errorf("failed to stop container (%w)", err)
		}
	}
	r.container = nil
	r.lock.Unlock()
	<-r.done
	return nil
}

func (r *runningStep) run() {
	defer func() {
		r.cancel()
		close(r.done)
	}()
	container, err := r.deployStage()
	if err != nil {
		r.deployFailed(err)
		return
	}
	r.lock.Lock()
	select {
	case <-r.ctx.Done():
		if err := container.Close(); err != nil {
			r.logger.Warningf("failed to remove deployed container for step %s", r.step)
		}
		r.lock.Unlock()
		return
	default:
		r.container = container
	}
	r.lock.Unlock()
	if err := r.startStage(container); err != nil {
		r.startFailed(err)
		return
	}
	if err := r.runStage(); err != nil {
		r.runFailed(err)
	}
}

func (r *runningStep) deployStage() (deployer.Plugin, error) {
	r.lock.Lock()
	if !r.deployInputAvailable {
		r.state = step.RunningStepStateWaitingForInput
	} else {
		r.state = step.RunningStepStateRunning
	}
	deployInputAvailable := r.deployInputAvailable
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		nil,
		nil,
		nil,
		string(StageIDDeploy),
		deployInputAvailable,
	)

	var deployerConfig any
	var useLocalDeployer bool
	select {
	case deployerConfig = <-r.deployInput:
		r.lock.Lock()
		r.state = step.RunningStepStateRunning
		useLocalDeployer = r.useLocalDeployer
		r.lock.Unlock()
	case <-r.ctx.Done():
		return nil, fmt.Errorf("step closed before deployment config could be obtained")
	}

	var stepDeployer = r.localDeployer
	if !useLocalDeployer {
		var err error
		stepDeployer, err = r.deployerRegistry.Create(deployerConfig, r.logger)
		if err != nil {
			return nil, err
		}
	}
	container, err := stepDeployer.Deploy(r.ctx, r.image)
	if err != nil {
		return nil, err
	}
	return container, nil
}

type executionResult struct {
	outputID   string
	outputData any
	err        error
}

func (r *runningStep) startStage(container deployer.Plugin) error {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDStarting

	if !r.runInputAvailable {
		r.state = step.RunningStepStateWaitingForInput
	} else {
		r.state = step.RunningStepStateRunning
	}
	runInputAvailable := r.runInputAvailable
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDStarting),
		runInputAvailable,
	)

	r.lock.Lock()
	r.currentStage = StageIDStarting
	r.state = step.RunningStepStateWaitingForInput
	r.lock.Unlock()

	var runInput any
	select {
	case runInput = <-r.runInput:
		r.lock.Lock()
		r.state = step.RunningStepStateRunning
		r.lock.Unlock()
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

	// Runs the ATP client in a goroutine in order to wait for it or context done.
	// On context done, the deployer tries to end execution. That will shut down
	// (with sigterm) the container. Then wait for output, or error out.
	go func() {
		outputID, outputData, err := atpClient.Execute(r.step, runInput)
		r.executionChannel <- executionResult{outputID, outputData, err}
	}()
	return nil
}

func (r *runningStep) runStage() error {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDRunning
	r.state = step.RunningStepStateRunning
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDRunning),
		false,
	)

	var result executionResult
	select {
	case result = <-r.executionChannel:
		if result.err != nil {
			return result.err
		}
	case <-r.ctx.Done():
		// In this case, it is being instructed to stop.
		// Shutdown (with sigterm) the container, then wait for the output (valid or error).
		r.logger.Debugf("Running step context done before step run complete. Cancelling and waiting for result.")
		r.cancel()
		// If necessary, you can add a timeout here for shutdowns that take too long.
		result = <-r.executionChannel
	}

	// Execution complete, move to finished stage.
	r.lock.Lock()
	// Be careful that everything here is set correctly.
	// Else it will cause undesired behavior.
	previousStage = string(r.currentStage)
	r.currentStage = StageIDOutput
	// First running, then state change, then finished.
	// This is so it properly steps through all the stages it needs to.
	r.state = step.RunningStepStateRunning
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(r.currentStage),
		false)

	r.lock.Lock()
	r.state = step.RunningStepStateFinished
	r.lock.Unlock()
	r.stageChangeHandler.OnStepComplete(
		r,
		string(r.currentStage),
		&result.outputID,
		&result.outputData,
	)

	return nil
}

func (r *runningStep) deployFailed(err error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDDeployFailed
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.
	r.state = step.RunningStepStateRunning
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDDeployFailed),
		false,
	)
	r.logger.Warningf("Plugin %s deploy failed. %v", r.step, err)

	// Now it's done.
	r.lock.Lock()
	r.currentStage = StageIDDeployFailed
	r.state = step.RunningStepStateFinished
	r.lock.Unlock()

	outputID := errorStr
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

func (r *runningStep) startFailed(err error) {
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
	r.logger.Warningf("Plugin step %s start failed. %v", r.step, err)

	// Now it's done.
	r.lock.Lock()
	r.currentStage = StageIDCrashed
	r.state = step.RunningStepStateFinished
	r.lock.Unlock()

	outputID := errorStr
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

func (r *runningStep) runFailed(err error) {
	// A current lack of observability into the atp client prevents
	// non-fragile testing of this function.

	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDCrashed
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.	r.state = step.RunningStepStateRunning
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(r.currentStage),
		false)

	r.logger.Warningf("Plugin step %s run failed. %v", r.step, err)

	// Now it's done.
	r.lock.Lock()
	r.currentStage = StageIDCrashed
	r.state = step.RunningStepStateFinished
	r.lock.Unlock()

	outputID := errorStr
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

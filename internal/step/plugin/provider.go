package plugin

import (
	"context"
	"fmt"
	"go.flow.arcalot.io/pluginsdk/plugin"
	"reflect"
	"strings"
	"sync"
	"time"

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
func New(logger log.Logger, deployerRegistry registry.Registry, localDeployerConfigs map[string]any) (step.Provider, error) {
	localDeployers := make(map[deployer.DeploymentType]deployer.Connector)

	// Build local deployers from requested deployers in engine workflow config.
	for reqDeploymentType, deployerConfig := range localDeployerConfigs {
		reqDeploymentTypeType := deployer.DeploymentType(reqDeploymentType)
		// Unserialize config using deployer's schema in registry.
		// This will return an error if the requested deployment type
		// is not in the registry.
		unserializedLocalDeployerConfig, err := deployerRegistry.DeployConfigSchema(
			reqDeploymentTypeType).Unserialize(deployerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load requested deployer type %s from workflow config (%w)",
				reqDeploymentType, err)
		}

		localDeployer, err := deployerRegistry.Create(reqDeploymentTypeType,
			unserializedLocalDeployerConfig, logger.WithLabel("source", "deployer"))
		if err != nil {
			return nil, fmt.Errorf("invalid local deployer configuration, please check your Arcaflow configuration file (%w)", err)
		}
		localDeployers[reqDeploymentTypeType] = localDeployer
	}

	return &pluginProvider{
		logger:           logger.WithLabel("source", "plugin-provider"),
		deployerRegistry: deployerRegistry,
		localDeployers:   localDeployers,
	}, nil
}

func (p *pluginProvider) Kind() string {
	return "plugin"
}

type pluginProvider struct {
	deployerRegistry registry.Registry
	localDeployers   map[deployer.DeploymentType]deployer.Connector
	logger           log.Logger
}

func (p *pluginProvider) Register(_ step.Registry) {
}

func keysString(m []deployer.DeploymentType) string {
	keys := make([]string, 0, len(m))
	for _, k := range m {
		keys = append(keys, string(k))
	}
	return "[" + strings.Join(keys, ", ") + "]"
}

func (p *pluginProvider) ProviderSchema() map[string]*schema.PropertySchema {
	return map[string]*schema.PropertySchema{
		"plugin": schema.NewPropertySchema(
			schema.NewObjectSchema(
				"plugin_fields",
				map[string]*schema.PropertySchema{
					"src": schema.NewPropertySchema(
						schema.NewStringSchema(schema.PointerTo[int64](1), nil, nil),
						schema.NewDisplayValue(
							schema.PointerTo("Source"),
							schema.PointerTo("Source file to be executed."), nil),
						true,
						nil,
						nil,
						nil,
						nil,
						[]string{"\"quay.io/arcaflow/example-plugin:latest\""},
					),
					"deployment_type": schema.NewPropertySchema(
						schema.NewStringSchema(schema.PointerTo[int64](1), nil, nil),
						schema.NewDisplayValue(
							schema.PointerTo("Type"),
							schema.PointerTo(
								fmt.Sprintf("Deployment type [%s]",
									keysString(p.deployerRegistry.DeploymentTypes()))),
							nil,
						),
						true,
						nil,
						nil,
						nil,
						nil,
						[]string{"image"},
					),
				},
			),
			schema.NewDisplayValue(
				schema.PointerTo("Plugin Info"),
				schema.PointerTo(
					fmt.Sprintf("Deployment type %s",
						keysString(p.deployerRegistry.DeploymentTypes()))),
				nil,
			),
			true,
			nil,
			nil,
			nil,
			nil,
			nil,
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
	// StageIDEnabling is a stage that indicates that the plugin is waiting to be enabled.
	// This is required to be separate to ensure that it exits immediately if disabled.
	StageIDEnabling StageID = "enabling"
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

var enablingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDEnabling),
	WaitingName:  "waiting to be enabled",
	RunningName:  "enabling",
	FinishedName: "enablement determined",
	InputFields: map[string]struct{}{
		"enabled": {},
	},
	NextStages: []string{
		string(StageIDStarting), string(StageIDOutput),
	},
}

var startingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDStarting),
	WaitingName:  "waiting to start",
	RunningName:  "starting",
	FinishedName: "started",
	InputFields: map[string]struct{}{
		"input":    {},
		"wait_for": {},
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

/*var disabledLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDisabled),
	WaitingName:  "waiting for the step to be disabled",
	RunningName:  "disabling",
	FinishedName: "disabled",
	InputFields:  map[string]struct{}{}, // The starting stage is the one that waits, so the input goes there instead.
	NextStages: []string{
		string(StageIDOutput),
	},
}*/

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

// Lifecycle returns a lifecycle that contains all plugin lifecycle stages.
func (p *pluginProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: string(StageIDDeploy),
		Stages: []step.LifecycleStage{
			deployingLifecycleStage,
			deployFailedLifecycleStage,
			enablingLifecycleStage,
			startingLifecycleStage,
			runningLifecycleStage,
			cancelledLifecycleStage,
			//disabledLifecycleStage,
			finishedLifecycleStage,
			crashedLifecycleStage,
		},
	}
}

// LoadSchema deploys the plugin, connects to the plugin's ATP server, loads its schema, then
// returns a runnableStep struct. Not to be confused with the runningStep struct.
func (p *pluginProvider) LoadSchema(inputs map[string]any, _ map[string][]byte) (step.RunnableStep, error) {
	pluginSrcInput := inputs["plugin"].(map[string]any)
	requestedDeploymentType := deployer.DeploymentType(pluginSrcInput["deployment_type"].(string))
	pluginSource := pluginSrcInput["src"].(string)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applicableLocalDeployer, ok := p.localDeployers[requestedDeploymentType]
	if !ok {
		return nil, fmt.Errorf("missing local deployer for requested type %s", requestedDeploymentType)
	}
	pluginConnector, err := applicableLocalDeployer.Deploy(ctx, pluginSource)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to deploy plugin of deployment type '%s' with source '%s' (%w)",
			requestedDeploymentType, pluginSource, err)
	}
	// Set up the ATP connection
	transport := atp.NewClientWithLogger(pluginConnector, p.logger)
	// Read the schema information
	s, err := transport.ReadSchema()
	if err != nil {
		cancel()
		// Close it. This allows it go get the error messages.
		deployerErr := pluginConnector.Close()
		if deployerErr != nil {
			return nil, fmt.Errorf("failed to read plugin schema from '%s' (%w). Deployer close error: (%s)",
				pluginSource, err, deployerErr.Error())
		}
		return nil, fmt.Errorf("failed to read plugin schema from '%s' (%w)",
			pluginSource, err)
	}
	// Tell the server that the client is done
	if err := transport.Close(); err != nil {
		return nil, fmt.Errorf("failed to instruct client to shut down plugin from source '%s' (%w)", pluginSource, err)
	}
	// Shut down the plugin.
	if err := pluginConnector.Close(); err != nil {
		return nil, fmt.Errorf("failed to shut down local plugin from '%s' (%w)", pluginSource, err)
	}

	return &runnableStep{
		schemas:          *s,
		logger:           p.logger,
		deploymentType:   requestedDeploymentType,
		source:           pluginSource,
		deployerRegistry: p.deployerRegistry,
		localDeployer:    applicableLocalDeployer,
	}, nil
}

type runnableStep struct {
	deploymentType   deployer.DeploymentType
	source           string
	deployerRegistry registry.Registry
	logger           log.Logger
	schemas          schema.SchemaSchema
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

func (r *runnableStep) StartedSchema() *schema.StepOutputSchema {
	return schema.NewStepOutputSchema(
		schema.NewScopeSchema(
			schema.NewObjectSchema(
				"StartedOutput",
				map[string]*schema.PropertySchema{},
			),
		),
		nil,
		false,
	)
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
			return result, fmt.Errorf("the 'step' parameter is required for the '%s' plugin", r.source)
		}
		for possibleStepID := range steps {
			stepID = possibleStepID
		}
	}
	stepSchema, ok := r.schemas.Steps()[stepID]
	if !ok {
		return result, fmt.Errorf("the step '%s' does not exist in the '%s' plugin", stepID, r.source)
	}

	stopIfProperty := schema.NewPropertySchema(
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
	)
	// Now validate that the step's internal dependencies can be resolved (like stop_if's dependency on the cancel signal)
	cancelSignal := stepSchema.SignalHandlers()[plugin.CancellationSignalSchema.ID()]
	if cancelSignal == nil {
		// Not present
		stopIfProperty.Disable(fmt.Sprintf("Cancel signal with ID '%s' is not present in plugin '%s', step '%s'. Signal handler IDs present: %v",
			plugin.CancellationSignalSchema.ID(), r.source, stepID, reflect.ValueOf(stepSchema.SignalHandlers()).MapKeys()))
	} else if err := plugin.CancellationSignalSchema.DataSchemaValue.ValidateCompatibility(cancelSignal.DataSchemaValue); err != nil {
		// Present but incompatible
		stopIfProperty.Disable(fmt.Sprintf("Cancel signal invalid schema in plugin '%s', step '%s' (%s)", r.source, stepID, err))
	}

	return step.Lifecycle[step.LifecycleStageWithSchema]{
		InitialStage: "deploying",
		Stages: []step.LifecycleStageWithSchema{
			{
				LifecycleStage: deployingLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
					"deploy": schema.NewPropertySchema(
						r.deployerRegistry.DeployConfigSchema(r.deploymentType),
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
				Outputs: nil,
			},
			{
				LifecycleStage: startingLifecycleStage,
				InputSchema: map[string]*schema.PropertySchema{
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
					"started": r.StartedSchema(),
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
					"stop_if": stopIfProperty,
				},
				Outputs: nil,
			},
			/*{
				LifecycleStage: disabledLifecycleStage,
				InputSchema:    nil,
				Outputs:        nil,
			},*/
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

func (r *runnableStep) Start(input map[string]any, runID string, stageChangeHandler step.StageChangeHandler) (step.RunningStep, error) {
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
				"the '%s' plugin declares more than one possible step, please provide the step name (one of: %s)",
				r.source,
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
			"plugin '%s' does not have a step named %s",
			r.source,
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
		deployInput:        make(chan any, 1),
		runInput:           make(chan any, 1),
		enabledInput:       make(chan bool, 1),
		logger:             r.logger,
		deploymentType:     r.deploymentType,
		source:             r.source,
		pluginStepID:       stepID,
		state:              step.RunningStepStateStarting,
		localDeployer:      r.localDeployer,
		executionChannel:   make(chan atp.ExecutionResult),
		signalToStep:       make(chan schema.Input),
		signalFromStep:     make(chan schema.Input),
		runID:              runID,
	}

	go s.run()

	return s, nil
}

type runningStep struct {
	deployerRegistry      registry.Registry
	stepSchema            schema.Step
	stageChangeHandler    step.StageChangeHandler
	lock                  *sync.Mutex
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	atpClient             atp.Client
	deployInput           chan any
	deployInputAvailable  bool
	enabledInput          chan bool
	enabledInputAvailable bool
	runInput              chan any
	runInputAvailable     bool
	logger                log.Logger
	currentStage          StageID
	runID                 string // The ID associated with this execution (the workflow step ID)
	deploymentType        deployer.DeploymentType
	source                string
	pluginStepID          string // The ID of the step in the plugin
	state                 step.RunningStepState
	useLocalDeployer      bool
	localDeployer         deployer.Connector
	container             deployer.Plugin
	executionChannel      chan atp.ExecutionResult
	signalToStep          chan schema.Input // Communicates with the ATP client, not other steps.
	signalFromStep        chan schema.Input // Communicates with the ATP client, not other steps.
	closed                bool
	// Store channels for sending pre-calculated signal outputs to other steps?
	// Store channels for receiving pre-calculated signal inputs from other steps?
}

func (r *runningStep) CurrentStage() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	tempStage := string(r.currentStage)
	return tempStage
}

func (r *runningStep) State() step.RunningStepState {
	r.lock.Lock()
	defer r.lock.Unlock()
	tempState := r.state
	return tempState
}

func (r *runningStep) ProvideStageInput(stage string, input map[string]any) error {
	// If you change the running step's Stage in this function it can
	// affect the counting of step states in the workflow's Execute function
	// and notifySteps function.
	r.lock.Lock()
	defer r.lock.Unlock()
	r.logger.Debugf("ProvideStageInput START")
	defer r.logger.Debugf("ProvideStageInput END")

	// Checks which stage it is getting input for, and handles it.
	switch stage {
	case string(StageIDDeploy):
		return r.provideDeployInput(input)
	case string(StageIDStarting):
		return r.provideStartingInput(input)
	case string(StageIDEnabling):
		return r.provideEnablingInput(input)
	case string(StageIDRunning):
		return nil
	case string(StageIDCancelled):
		return r.provideCancelledInput(input)
	case string(StageIDDeployFailed):
		return nil
	case string(StageIDCrashed):
		return nil
	case string(StageIDOutput):
		return nil
	default:
		return fmt.Errorf("bug: invalid stage: %s", stage)
	}
}

func (r *runningStep) provideDeployInput(input map[string]any) error {
	// Note: The calling function must have the step mutex locked
	// input provided on this call overwrites the deployer configuration
	// set at this plugin provider's instantiation
	if r.deployInputAvailable {
		return fmt.Errorf("deployment information provided more than once")
	}
	var unserializedDeployerConfig any
	var err error
	if input["deploy"] != nil {
		unserializedDeployerConfig, err = r.deployerRegistry.DeployConfigSchema(r.deploymentType).Unserialize(input["deploy"])
		if err != nil {
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

	// Feed the deploy step its input.
	select {
	case r.deployInput <- unserializedDeployerConfig:
	default:
		return fmt.Errorf("unable to provide input to deploy stage for step %s/%s", r.runID, r.pluginStepID)
	}
	return nil
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

func (r *runningStep) provideStartingInput(input map[string]any) error {
	// Note: The calling function must have the step mutex locked
	if r.runInputAvailable {
		return fmt.Errorf("starting input provided more than once")
	}
	// Ensure input is given
	if input["input"] == nil {
		return fmt.Errorf("bug: invalid input for 'running' stage, expected 'input' field")
	}
	// Validate the input by unserializing it
	if _, err := r.stepSchema.Input().Unserialize(input["input"]); err != nil {
		return err
	}
	// Make sure we transition the state before unlocking so there are no race conditions.
	r.runInputAvailable = true

	// Unlock before passing the data over the channel to prevent a deadlock.
	// The other end of the channel needs to be unlocked to read the data.

	// Feed the run step its input over the channel.
	select {
	case r.runInput <- input["input"]:
	default:
		return fmt.Errorf("unable to provide input to run stage for step %s/%s", r.runID, r.pluginStepID)
	}
	return nil
}

func (r *runningStep) provideCancelledInput(input map[string]any) error {
	// Note: The calling function must have the step mutex locked
	// Cancel if the step field is present and isn't false
	if input["stop_if"] != false && input["stop_if"] != nil {
		r.cancelStep()
	}
	return nil
}

// cancelStep gracefully requests cancellation for any stage.
// If running, it sends a cancel signal if the plugin supports it.
func (r *runningStep) cancelStep() {
	r.logger.Infof("Cancelling step %s/%s", r.runID, r.pluginStepID)
	// We only need to call the signal if the step is running.
	// If it isn't, cancelling the context alone should be enough.
	if r.currentStage == StageIDRunning {
		// Verify that the step has a cancel signal
		cancelSignal := r.stepSchema.SignalHandlers()[plugin.CancellationSignalSchema.ID()]
		if cancelSignal == nil {
			r.logger.Errorf("could not cancel step %s/%s. Does not contain cancel signal receiver.", r.runID, r.pluginStepID)
		} else if err := plugin.CancellationSignalSchema.DataSchema().ValidateCompatibility(cancelSignal.DataSchema()); err != nil {
			r.logger.Errorf("validation failed for cancel signal for step %s/%s: %s", r.runID, r.pluginStepID, err)
		} else {
			// Validated. Now call the signal.
			r.signalToStep <- schema.Input{RunID: r.runID, ID: cancelSignal.ID(), InputData: map[any]any{}}
		}
	}
	// Now cancel the context to stop the non-running parts of the step
	r.cancel()
}

// ForceClose closes the step without waiting for a graceful shutdown of the ATP client.
// Warning: This means that it won't wait for the ATP client to finish. This is okay if using a deployer that
// will stop execution once the deployer closes it.
func (r *runningStep) ForceClose() error {
	err := r.closeComponents(false)
	// Wait for the run to finish to ensure that it's not running after closing.
	r.wg.Wait()
	r.closed = true
	r.logger.Warningf("Step %s/%s force closed.", r.runID, r.pluginStepID)
	return err
}

func (r *runningStep) Close() error {
	err := r.closeComponents(true)
	// Wait for the run to finish to ensure that it's not running after closing.
	r.wg.Wait()
	r.closed = true
	return err
}

func (r *runningStep) closeComponents(closeATP bool) error {
	r.cancel()
	r.lock.Lock()
	if r.closed {
		return nil // Already closed
	}
	var atpErr error
	var containerErr error
	if r.atpClient != nil && closeATP {
		atpErr = r.atpClient.Close()
	}
	if r.container != nil {
		containerErr = r.container.Close()
	}
	r.container = nil
	r.lock.Unlock()
	if containerErr != nil {
		return fmt.Errorf("error while stopping container (%w)", containerErr)
	} else if atpErr != nil {
		return fmt.Errorf("error while stopping atp client (%w)", atpErr)
		// Do not wait in this case. It may never get resolved.
	}
	return nil
}

func (r *runningStep) run() {
	r.wg.Add(1) // Wait for the run to finish before closing.
	defer func() {
		r.cancel()  // Close before WaitGroup done
		r.wg.Done() // Done. Close may now exit.
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
			r.logger.Warningf("failed to remove deployed container for step %s/%s", r.runID, r.pluginStepID)
		}
		r.lock.Unlock()
		r.transitionToCancelled()
		return
	default:
		r.container = container
	}
	r.lock.Unlock()
	r.logger.Debugf("Successfully deployed container with ID '%s' for step %s/%s", container.ID(), r.runID, r.pluginStepID)

	r.logger.Debugf("Checking to see if step %s/%s is enabled", r.runID, r.pluginStepID)
	enabled, err := r.enableStage()
	r.logger.Debugf("Step %s/%s enablement state: %t", r.runID, r.pluginStepID, enabled)
	if err != nil {
		// TODO
		r.logger.Errorf("TODO: Error while checking stage enablement")
	}
	if !enabled {
		r.startFailed(fmt.Errorf("step disabled; TODO: handle this more gracefully"))
		return
	}
	if err := r.startStage(container); err != nil {
		r.startFailed(err)
		return
	}
	if err := r.runStage(); err != nil {
		r.runFailed(err)
	}
}

func (r *runningStep) deployStage() (deployer.Plugin, error) {
	r.logger.Debugf("Deploying stage for step %s/%s", r.runID, r.pluginStepID)
	r.lock.Lock()
	r.state = step.RunningStepStateRunning
	deployInputAvailable := r.deployInputAvailable
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		nil,
		nil,
		nil,
		string(StageIDDeploy),
		deployInputAvailable,
		&r.wg,
	)

	var deployerConfig any
	var useLocalDeployer bool
	// First, non-blocking retrieval
	select {
	case deployerConfig = <-r.deployInput:
		r.lock.Lock()
		r.state = step.RunningStepStateRunning
		r.lock.Unlock()
	default: // Default, so it doesn't block on this receive
		// It's waiting now.
		r.lock.Lock()
		r.state = step.RunningStepStateWaitingForInput
		r.lock.Unlock()
		select {
		case deployerConfig = <-r.deployInput:
			r.lock.Lock()
			r.state = step.RunningStepStateRunning
			r.lock.Unlock()
		case <-r.ctx.Done():
			return nil, fmt.Errorf("step closed before deployment config could be obtained")
		}
	}
	r.lock.Lock()
	useLocalDeployer = r.useLocalDeployer
	r.lock.Unlock()

	var stepDeployer = r.localDeployer
	if !useLocalDeployer {
		var err error
		stepDeployer, err = r.deployerRegistry.Create(r.deploymentType, deployerConfig,
			r.logger.WithLabel("source", "deployer"))
		if err != nil {
			return nil, err
		}
	}
	container, err := stepDeployer.Deploy(r.ctx, r.source)
	if err != nil {
		return nil, err
	}
	return container, nil
}

// enableStage returns the result of whether the stage was enabled or not.
func (r *runningStep) enableStage() (bool, error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDEnabling
	enabledInputAvailable := r.enabledInputAvailable
	r.lock.Unlock()

	r.stageChangeHandler.OnStageChange(
		r,
		&previousStage,
		nil,
		nil,
		string(StageIDEnabling),
		enabledInputAvailable,
		&r.wg,
	)

	select {
	case enabled := <-r.enabledInput:
		return enabled, nil
	case <-r.ctx.Done():
		return false, fmt.Errorf("step closed while determining enablement status")
	}
}

func (r *runningStep) startStage(container deployer.Plugin) error {
	r.logger.Debugf("Starting stage for step %s/%s", r.runID, r.pluginStepID)
	atpClient := atp.NewClientWithLogger(container, r.logger)
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDStarting
	inputRecievedEarly := false
	r.atpClient = atpClient

	var runInput any
	select {
	case runInput = <-r.runInput:
		// Good. It received it immediately.
		r.state = step.RunningStepStateRunning
		inputRecievedEarly = true
	default: // The default makes it not wait.
		r.state = step.RunningStepStateWaitingForInput
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
		&r.wg,
	)

	r.lock.Lock()
	r.currentStage = StageIDStarting
	r.logger.Debugf("Waiting for input state while starting 2.")
	r.lock.Unlock()

	// First, try to non-blocking retrieve the runInput.
	// If not yet available, set to state waiting for input and do a blocking receive.
	// If it is available, continue.
	if !inputRecievedEarly {
		// Input is not yet available. Now waiting.
		r.lock.Lock()
		if r.state != step.RunningStepStateWaitingForInput {
			r.logger.Warningf("State not waiting for input when receiving from channel.")
		}
		r.lock.Unlock()

		// Do a blocking wait for input now.
		select {
		case runInput = <-r.runInput:
			r.lock.Lock()
			r.state = step.RunningStepStateRunning
			r.lock.Unlock()
		case <-r.ctx.Done():
			return fmt.Errorf("step closed while waiting for run configuration")
		}
	}

	inputSchema, err := r.atpClient.ReadSchema()
	if err != nil {
		return err
	}
	steps := inputSchema.Steps()
	stepSchema, ok := steps[r.pluginStepID]
	if !ok {
		return fmt.Errorf("error in run step %s: schema mismatch between local and remote deployed plugin, no stepSchema named %s found in remote", r.runID, r.pluginStepID)
	}
	// Re-verify input. This should have also been done earlier.
	if _, err := stepSchema.Input().Unserialize(runInput); err != nil {
		return fmt.Errorf("schema mismatch between local and remote deployed plugin in step %s/%s, unserializing input failed (%w)", r.runID, r.pluginStepID, err)
	}

	// Runs the ATP client in a goroutine in order to wait for it.
	// On context done, the deployer has 30 seconds before it will error out.
	go func() {
		result := r.atpClient.Execute(
			schema.Input{RunID: r.runID, ID: r.pluginStepID, InputData: runInput},
			r.signalToStep,
			r.signalFromStep,
		)
		r.executionChannel <- result
		if err = r.atpClient.Close(); err != nil {
			r.logger.Warningf("Error while closing ATP client: %s", err)
		}
	}()
	return nil
}

func (r *runningStep) runStage() error {
	r.logger.Debugf("Running stage for step %s/%s", r.runID, r.pluginStepID)
	startedOutput := any(map[any]any{})
	r.transitionStageWithOutput(StageIDRunning, step.RunningStepStateRunning, schema.PointerTo("started"), &startedOutput)

	var result atp.ExecutionResult
	select {
	case result = <-r.executionChannel:
		if result.Error != nil {
			return result.Error
		}
	case <-r.ctx.Done():
		// In this case, it is being instructed to stop. A signal should have been sent.
		// Shutdown (with sigterm) the container, then wait for the output (valid or error).
		r.logger.Debugf("Got step context done before step run complete. Waiting up to 30 seconds for result.")
		select {
		case result = <-r.executionChannel:
			// Successfully stopped before end of timeout.
		case <-time.After(time.Duration(30) * time.Second):
			r.logger.Warningf("Step %s/%s did not complete within the 30 second time limit. Force closing container.",
				r.runID, r.pluginStepID)
			if err := r.ForceClose(); err != nil {
				r.logger.Warningf("Error in step %s/%s while closing plugin container (%w)", r.runID, r.pluginStepID, err)
			}
		}

	}

	// Execution complete, move to state running stage outputs, then to state finished stage.
	r.transitionStage(StageIDOutput, step.RunningStepStateRunning)
	r.completeStep(r.currentStage, step.RunningStepStateFinished, &result.OutputID, &result.OutputData)

	return nil
}

func (r *runningStep) deployFailed(err error) {
	r.logger.Debugf("Deploy failed stage for step %s/%s", r.runID, r.pluginStepID)
	r.transitionStage(StageIDDeployFailed, step.RunningStepStateRunning)
	r.logger.Warningf("Plugin step %s/%s deploy failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(DeployFailed{
		Error: err.Error(),
	})
	r.completeStep(StageIDDeployFailed, step.RunningStepStateFinished, &outputID, &output)
}

func (r *runningStep) transitionToCancelled() {
	r.logger.Infof("Step %s/%s cancelled", r.runID, r.pluginStepID)
	// Follow the convention of transitioning to running then finished.
	r.transitionStage(StageIDCancelled, step.RunningStepStateRunning)
	// Cancelled currently has no output.
	r.transitionStage(StageIDCancelled, step.RunningStepStateFinished)
}

func (r *runningStep) startFailed(err error) {
	r.logger.Debugf("Start failed stage for step %s/%s", r.runID, r.pluginStepID)
	r.transitionStage(StageIDCrashed, step.RunningStepStateRunning)
	r.logger.Warningf("Plugin step %s/%s start failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(Crashed{
		Output: err.Error(),
	})

	r.completeStep(StageIDCrashed, step.RunningStepStateFinished, &outputID, &output)
}

func (r *runningStep) runFailed(err error) {
	r.logger.Debugf("Run failed stage for step %s/%s", r.runID, r.pluginStepID)
	r.transitionStage(StageIDCrashed, step.RunningStepStateRunning)
	r.logger.Warningf("Plugin step %s/%s run failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(Crashed{
		Output: err.Error(),
	})
	r.completeStep(StageIDCrashed, step.RunningStepStateFinished, &outputID, &output)
}

// TransitionStage transitions the stage to the specified stage, and the state to the specified state.
func (r *runningStep) transitionStage(newStage StageID, state step.RunningStepState) {
	r.transitionStageWithOutput(newStage, state, nil, nil)
}

// TransitionStage transitions the stage to the specified stage, and the state to the specified state.
func (r *runningStep) transitionStageWithOutput(newStage StageID, state step.RunningStepState, outputID *string, previousStageOutput *any) {
	// A current lack of observability into the atp client prevents
	// non-fragile testing of this function.
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = newStage
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.
	r.state = state
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
	r.state = state
	r.lock.Unlock()

	r.stageChangeHandler.OnStepComplete(
		r,
		previousStage,
		outputID,
		previousStageOutput,
		&r.wg,
	)
}

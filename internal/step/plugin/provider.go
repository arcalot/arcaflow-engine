package plugin

import (
	"context"
	"fmt"
	"go.arcalot.io/dgraph"
	"go.flow.arcalot.io/pluginsdk/plugin"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.arcalot.io/log/v2"
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
	// StageIDDisabled is a stage that indicates that the plugin's step was disabled.
	StageIDDisabled StageID = "disabled"
	// StageIDCancelled is a stage that indicates that the plugin's step was cancelled.
	StageIDCancelled StageID = "cancelled"
	// StageIDOutput is a stage that indicates that the plugin has completed working successfully.
	StageIDOutput StageID = "outputs"
	// StageIDCrashed is a stage that indicates that the plugin has quit unexpectedly.
	StageIDCrashed StageID = "crashed"
	// StageIDClosed is a stage that indicates that the plugin has exited due to workflow termination.
	StageIDClosed StageID = "closed"
	// StageIDStarting is a stage that indicates that the plugin execution has begun.
	StageIDStarting StageID = "starting"
)

var deployingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDeploy),
	WaitingName:  "waiting for deployment",
	RunningName:  "deploying",
	FinishedName: "deployed",
	InputFields:  map[string]struct{}{string(StageIDDeploy): {}},
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDStarting):     dgraph.AndDependency,
		string(StageIDDeployFailed): dgraph.CompletionAndDependency,
		string(StageIDClosed):       dgraph.CompletionAndDependency,
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
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDStarting): dgraph.AndDependency,
		string(StageIDDisabled): dgraph.AndDependency,
		string(StageIDCrashed):  dgraph.CompletionAndDependency,
		string(StageIDClosed):   dgraph.CompletionAndDependency,
	},
}

var startingLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDStarting),
	WaitingName:  "waiting to start",
	RunningName:  "starting",
	FinishedName: "started",
	InputFields: map[string]struct{}{
		"input":                {},
		"wait_for":             {},
		"closure_wait_timeout": {},
	},
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDRunning): dgraph.AndDependency,
		string(StageIDCrashed): dgraph.CompletionAndDependency,
		string(StageIDClosed):  dgraph.CompletionAndDependency,
	},
}

var runningLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDRunning),
	WaitingName:  "waiting to run",
	RunningName:  "running",
	FinishedName: "completed",
	InputFields:  map[string]struct{}{},
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDOutput):  dgraph.AndDependency,
		string(StageIDCrashed): dgraph.CompletionAndDependency,
		string(StageIDClosed):  dgraph.CompletionAndDependency,
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
	NextStages: map[string]dgraph.DependencyType{
		string(StageIDOutput):       dgraph.CompletionAndDependency,
		string(StageIDCrashed):      dgraph.CompletionAndDependency,
		string(StageIDDeployFailed): dgraph.CompletionAndDependency,
		string(StageIDClosed):       dgraph.CompletionAndDependency,
	},
}

var disabledLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDDisabled),
	WaitingName:  "waiting for the step to be disabled",
	RunningName:  "disabling",
	FinishedName: "disabled",
	InputFields:  map[string]struct{}{},
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
var closedLifecycleStage = step.LifecycleStage{
	ID:           string(StageIDClosed),
	WaitingName:  "closed",
	RunningName:  "closed",
	FinishedName: "closed",
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
			disabledLifecycleStage,
			finishedLifecycleStage,
			crashedLifecycleStage,
			closedLifecycleStage,
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
	transport := atp.NewClientWithLogger(pluginConnector, p.logger.WithLabel("source", "atp-client"))
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

func (r *runnableStep) EnabledOutputSchema() *schema.StepOutputSchema {
	return schema.NewStepOutputSchema(
		schema.NewScopeSchema(
			schema.NewObjectSchema(
				"EnabledOutput",
				map[string]*schema.PropertySchema{
					"enabled": schema.NewPropertySchema(
						schema.NewBoolSchema(),
						schema.NewDisplayValue(
							schema.PointerTo("enabled"),
							schema.PointerTo("Whether the step was enabled"),
							nil),
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
		nil,
		false,
	)
}

func (r *runnableStep) DisabledOutputSchema() *schema.StepOutputSchema {
	return schema.NewStepOutputSchema(
		schema.NewScopeSchema(
			schema.NewObjectSchema(
				"DisabledMessageOutput",
				map[string]*schema.PropertySchema{
					"message": schema.NewPropertySchema(
						schema.NewStringSchema(nil, nil, nil),
						schema.NewDisplayValue(
							schema.PointerTo("message"),
							schema.PointerTo("A human readable message stating that the step was disabled."),
							nil),
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
		nil,
		false,
	)
}

func closureTimeoutSchema() *schema.PropertySchema {
	return schema.NewPropertySchema(
		schema.NewIntSchema(schema.PointerTo(int64(0)), nil, nil),
		schema.NewDisplayValue(
			schema.PointerTo("closure wait timeout"),
			schema.PointerTo("The amount of milliseconds to wait after sending the cancel "+
				"signal on closure before force killing the step."),
			nil,
		),
		false,
		nil,
		nil,
		nil,
		schema.PointerTo("5000"), // Default 5 seconds.
		nil,
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
				Outputs: map[string]*schema.StepOutputSchema{
					"resolved": r.EnabledOutputSchema(),
				},
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
					"closure_wait_timeout": closureTimeoutSchema(),
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
			{
				LifecycleStage: disabledLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"output": r.DisabledOutputSchema(),
				},
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
			{
				LifecycleStage: closedLifecycleStage,
				InputSchema:    nil,
				Outputs: map[string]*schema.StepOutputSchema{
					"result": {
						SchemaValue: schema.NewScopeSchema(
							schema.NewObjectSchema(
								"ClosedInfo",
								map[string]*schema.PropertySchema{
									"cancelled": schema.NewPropertySchema(
										schema.NewBoolSchema(),
										schema.NewDisplayValue(
											schema.PointerTo("cancelled"),
											schema.PointerTo("Whether the step was cancelled by stop_if"),
											nil,
										),
										true,
										nil,
										nil,
										nil,
										nil,
										nil,
									),
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
		runInput:           make(chan runInput, 1),
		enabledInput:       make(chan bool, 1),
		logger:             r.logger,
		deploymentType:     r.deploymentType,
		source:             r.source,
		pluginStepID:       stepID,
		state:              step.RunningStepStateStarting,
		localDeployer:      r.localDeployer,
		executionChannel:   make(chan atp.ExecutionResult, 2), // Buffer in case a result happens after context done.
		signalToStep:       make(chan schema.Input, 10),
		signalFromStep:     make(chan schema.Input, 10),
		runID:              runID,
	}

	s.wg.Add(1) // Wait for the run to finish before closing.
	go s.run()

	return s, nil
}

type runInput struct {
	stepInputData       any
	forceCloseTimeoutMS int64
}

type runningStep struct {
	deployerRegistry      registry.Registry
	stepSchema            schema.Step
	stageChangeHandler    step.StageChangeHandler
	lock                  *sync.Mutex
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	cancelled             bool
	atpClient             atp.Client
	deployInput           chan any
	deployInputAvailable  bool
	enabledInput          chan bool
	enabledInputAvailable bool
	runInput              chan runInput
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
	closed                atomic.Bool
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
	case string(StageIDClosed):
		return nil
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

	// Now get the other necessary data for running the step
	var timeout int64
	if input["closure_wait_timeout"] != nil {
		unserializedTimeout, err := closureTimeoutSchema().Unserialize(input["closure_wait_timeout"])
		if err != nil {
			return err
		}
		timeout = unserializedTimeout.(int64)
	} else {
		timeout = 5000
	}

	// Make sure we transition the state before unlocking so there are no race conditions.
	r.runInputAvailable = true

	// Unlock before passing the data over the channel to prevent a deadlock.
	// The other end of the channel needs to be unlocked to read the data.

	// Feed the run step its input over the channel.
	select {
	case r.runInput <- runInput{
		stepInputData:       input["input"],
		forceCloseTimeoutMS: timeout,
	}:
	default:
		return fmt.Errorf("unable to provide input to run stage for step %s/%s", r.runID, r.pluginStepID)
	}
	return nil
}

func (r *runningStep) provideCancelledInput(input map[string]any) error {
	// Note: The calling function must have the step mutex locked
	// Cancel if the step field is present and isn't false
	if input["stop_if"] == nil {
		return nil
	}
	if input["stop_if"] != false {
		r.cancelled = true
		r.cancelStep()
	}
	return nil
}

func (r *runningStep) hasCancellationHandler() bool {
	handler, present := r.stepSchema.SignalHandlers()[plugin.CancellationSignalSchema.ID()]
	return present && handler != nil
}

func (r *runningStep) getCancellationHandler() *schema.SignalSchema {
	return r.stepSchema.SignalHandlers()[plugin.CancellationSignalSchema.ID()]
}

// cancelStep gracefully requests cancellation for any stage.
// If running, it sends a cancel signal if the plugin supports it.
func (r *runningStep) cancelStep() {
	r.logger.Infof("Cancelling step %s/%s", r.runID, r.pluginStepID)
	// We only need to call the signal if the step is running.
	// If it isn't, cancelling the context alone should be enough.
	if r.currentStage == StageIDRunning {
		// Verify that the step has a cancel signal
		if !r.hasCancellationHandler() {
			r.logger.Errorf("could not cancel step %s/%s. Does not contain cancel signal receiver.", r.runID, r.pluginStepID)
		}
		cancelSignal := r.getCancellationHandler()
		if err := plugin.CancellationSignalSchema.DataSchema().ValidateCompatibility(cancelSignal.DataSchema()); err != nil {
			r.logger.Errorf("validation failed for cancel signal for step %s/%s: %s", r.runID, r.pluginStepID, err)
		} else if r.signalToStep == nil {
			r.logger.Debugf("signal send channel closed; the step %s/%s likely finished", r.runID, r.pluginStepID)
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
	closedAlready := r.closed.Swap(true)
	if closedAlready {
		r.wg.Wait()
		return nil
	}
	err := r.forceClose()
	if err != nil {
		return err
	}
	// Wait for the run to finish to ensure that it's not running after closing.
	r.wg.Wait()
	r.logger.Warningf("Step %s/%s force closed.", r.runID, r.pluginStepID)
	return nil
}

// This is necessary so that the waitgroup's Wait() function is not called by a function whose
// completion is required to end the wait.
func (r *runningStep) forceCloseInternal() error {
	closedAlready := r.closed.Swap(true)
	if closedAlready {
		return nil
	}
	return r.forceClose()
}

func (r *runningStep) forceClose() error {
	r.closed.Store(true)
	r.cancel()
	err := r.closeComponents(false)
	return err
}

func (r *runningStep) Close() error {
	closedAlready := r.closed.Swap(true)
	if closedAlready {
		r.wg.Wait()
		return nil
	}
	r.cancel()
	err := r.closeComponents(true)
	// Wait for the run to finish to ensure that it's not running after closing.
	r.wg.Wait()
	r.closed.Store(true)
	return err
}

func (r *runningStep) closeComponents(closeATP bool) error {
	r.cancel()
	r.lock.Lock()
	if r.closed.Load() {
		r.lock.Unlock()
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

// Note: Caller must add 1 to the waitgroup before calling.
func (r *runningStep) run() {
	defer func() {
		r.cancel()  // Close before WaitGroup done
		r.wg.Done() // Done. Close may now exit.
	}()
	pluginConnection := r.startPlugin()
	if pluginConnection == nil {
		return
	}
	defer func() {
		err := pluginConnection.Close()
		if err != nil {
			r.logger.Errorf("failed to close deployed container for step %s/%s", r.runID, r.pluginStepID)
		}
	}()
	r.postDeployment(pluginConnection)
}

// Deploys the plugin, and handles failure cases.
func (r *runningStep) startPlugin() deployer.Plugin {
	pluginConnection, contextDoneEarly, err := r.deployStage()
	if contextDoneEarly {
		if err != nil {
			r.logger.Debugf("error due to step early closure: %s", err.Error())
		}
		r.closedEarly(StageIDEnabling, true)
		return nil
	} else if err != nil {
		r.deployFailed(err)
		return nil
	}
	r.lock.Lock()
	select {
	case <-r.ctx.Done():
		if err := pluginConnection.Close(); err != nil {
			r.logger.Warningf("failed to close deployed container for step %s/%s", r.runID, r.pluginStepID)
		}
		r.lock.Unlock()
		r.closedEarly(StageIDEnabling, false)
		return nil
	default:
		r.container = pluginConnection
	}
	r.lock.Unlock()
	r.logger.Debugf("Successfully deployed container with ID '%s' for step %s/%s", pluginConnection.ID(), r.runID, r.pluginStepID)
	return pluginConnection
}

func (r *runningStep) postDeployment(pluginConnection deployer.Plugin) {
	r.logger.Debugf("Checking to see if step %s/%s is enabled", r.runID, r.pluginStepID)
	enabled, contextDoneEarly := r.enableStage()
	if contextDoneEarly {
		r.closedEarly(StageIDStarting, true)
		return
	}
	r.logger.Debugf("Step %s/%s enablement state: %t", r.runID, r.pluginStepID, enabled)
	if !enabled {
		r.transitionToDisabled()
		return
	}

	var forceCloseTimeoutMS int64
	var err error
	if contextDoneEarly, forceCloseTimeoutMS, err = r.startStage(pluginConnection); contextDoneEarly {
		r.closedEarly(StageIDRunning, true)
		return
	} else if err != nil {
		r.startFailed(err)
		return
	}
	if err := r.runStage(forceCloseTimeoutMS); err != nil {
		r.runFailed(err)
	}
}

// deployStage deploys the step.
// Return types:
// - deployer.Plugin: The deployer's connection to the plugin.
// - bool: True if the context was done, causing early failure.
// - err: The error, if a failure occurred.
func (r *runningStep) deployStage() (deployer.Plugin, bool, error) {
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
			return nil, true, nil
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
			return nil, false, err
		}
	}
	container, err := stepDeployer.Deploy(r.ctx, r.source)
	if err != nil {
		return nil, false, err
	}
	return container, false, nil
}

// enableStage returns the result of whether the stage was enabled or not.
// Return values:
// - bool: Whether the step was enabled.
// - bool: True if the step was disabled due to context done.
func (r *runningStep) enableStage() (bool, bool) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = StageIDEnabling
	enabledInputAvailable := r.enabledInputAvailable
	r.state = step.RunningStepStateWaitingForInput
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

// startStage gets the inputs and starts the step.
// Return values:
// - bool: True if the step was closed early due to context done.
// - int64: the amount of milliseconds to wait to force terminate a step.
// - error: Any error that occurred while trying to start the step.
func (r *runningStep) startStage(container deployer.Plugin) (bool, int64, error) {
	r.logger.Debugf("Starting stage for step %s/%s", r.runID, r.pluginStepID)
	atpClient := atp.NewClientWithLogger(container, r.logger.WithLabel("source", "atp-client"))
	var inputReceivedEarly bool
	r.lock.Lock()
	r.atpClient = atpClient
	r.lock.Unlock()

	var runInput runInput
	var newState step.RunningStepState
	select {
	case runInput = <-r.runInput:
		// Good. It received it immediately.
		newState = step.RunningStepStateRunning
		inputReceivedEarly = true
	default: // The default makes it not wait.
		newState = step.RunningStepStateWaitingForInput
		inputReceivedEarly = false
	}

	enabledOutput := any(map[any]any{"enabled": true})
	// End Enabling with resolved output, and start starting
	r.transitionStageWithOutput(
		StageIDStarting,
		newState,
		schema.PointerTo("resolved"),
		&enabledOutput,
	)

	// First, try to non-blocking retrieve the runInput.
	// If not yet available, set to state waiting for input and do a blocking receive.
	// If it is available, continue.
	if !inputReceivedEarly {
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
			r.logger.Debugf("step closed while waiting for run configuration")
			return true, 0, nil
		}
	}

	inputSchema, err := r.atpClient.ReadSchema()
	if err != nil {
		return false, 0, err
	}
	steps := inputSchema.Steps()
	stepSchema, ok := steps[r.pluginStepID]
	if !ok {
		return false, 0, fmt.Errorf("error in run step %s: schema mismatch between local and remote deployed plugin, no stepSchema named %s found in remote", r.runID, r.pluginStepID)
	}
	// Re-verify input. This should have also been done earlier.
	if _, err := stepSchema.Input().Unserialize(runInput.stepInputData); err != nil {
		return false, 0, fmt.Errorf("schema mismatch between local and remote deployed plugin in step %s/%s, unserializing input failed (%w)", r.runID, r.pluginStepID, err)
	}

	r.wg.Add(1)

	// Runs the ATP client in a goroutine in order to wait for it.
	// On context done, the deployer has limited time before it will error out.
	go func() {
		defer r.wg.Done()
		result := r.atpClient.Execute(
			schema.Input{RunID: r.runID, ID: r.pluginStepID, InputData: runInput.stepInputData},
			r.signalToStep,
			r.signalFromStep,
		)
		r.lock.Lock()
		// The sender should be the one to close the signal send channel
		channel := r.signalToStep
		r.signalToStep = nil
		close(channel)
		r.lock.Unlock()
		r.executionChannel <- result
		if err = r.atpClient.Close(); err != nil {
			r.logger.Warningf("Error while closing ATP client: %s", err)
		}
	}()
	return false, runInput.forceCloseTimeoutMS, nil
}

func (r *runningStep) runStage(forCloseWaitMS int64) error {
	r.logger.Debugf("Running stage for step %s/%s", r.runID, r.pluginStepID)
	startedOutput := any(map[any]any{})
	r.transitionStageWithOutput(StageIDRunning, step.RunningStepStateRunning, schema.PointerTo("started"), &startedOutput)

	var result atp.ExecutionResult
	select {
	case result = <-r.executionChannel:
	case <-r.ctx.Done():
		// In this case, it is being instructed to stop. A cancellation signal should be sent if supported.
		if r.hasCancellationHandler() {
			r.logger.Debugf("Got step context done before step run complete. Sending cancellation signal. Waiting up to %d milliseconds for result.", forCloseWaitMS)
			r.lock.Lock()
			r.cancelStep()
			r.lock.Unlock()
		} else {
			r.logger.Warningf("Got step context done before step run complete. Force closing step %s/%s.", r.runID, r.pluginStepID)
			err := r.forceCloseInternal()
			return fmt.Errorf("step forced closed after timeout without result (%w)", err)
		}
		// Wait for cancellation to occur.
		select {
		case result = <-r.executionChannel:
			// Successfully stopped before end of timeout.
		case <-time.After(time.Duration(forCloseWaitMS) * time.Millisecond):
			r.logger.Warningf("Cancelled step %s/%s did not complete within the %d millisecond time limit. Force closing container.",
				r.runID, r.pluginStepID, forCloseWaitMS)
			if err := r.forceCloseInternal(); err != nil {
				r.logger.Warningf("Error in step %s/%s while closing plugin container (%s)", r.runID, r.pluginStepID, err.Error())
				return fmt.Errorf("step closed after timeout without result with error (%s)", err.Error())
			}
			return fmt.Errorf("step closed after timeout without result")
		}
	}

	if result.Error != nil {
		return result.Error
	}

	// Execution complete, move to state running stage outputs, then to state finished stage.
	r.transitionRunningStage(StageIDOutput)
	r.completeStep(r.currentStage, step.RunningStepStateFinished, &result.OutputID, &result.OutputData)

	return nil
}

func (r *runningStep) markStageFailures(firstStage StageID, err error) {
	switch firstStage {
	case StageIDEnabling:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDEnabling), &r.wg, err)
		fallthrough
	case StageIDDisabled:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDDisabled), &r.wg, err)
		fallthrough
	case StageIDStarting:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDStarting), &r.wg, err)
		fallthrough
	case StageIDRunning:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDRunning), &r.wg, err)
		fallthrough
	case StageIDOutput:
		r.stageChangeHandler.OnStepStageFailure(r, string(StageIDOutput), &r.wg, err)
	default:
		panic("unknown StageID")
	}
}

// Closable is the graceful case, so this is necessary if it crashes.
func (r *runningStep) markNotClosable(err error) {
	r.stageChangeHandler.OnStepStageFailure(r, string(StageIDClosed), &r.wg, err)
}

func (r *runningStep) deployFailed(err error) {
	r.logger.Debugf("Deploy failed stage for step %s/%s", r.runID, r.pluginStepID)
	r.transitionRunningStage(StageIDDeployFailed)
	r.logger.Warningf("Plugin step %s/%s deploy failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(DeployFailed{
		Error: err.Error(),
	})
	r.completeStep(StageIDDeployFailed, step.RunningStepStateFinished, &outputID, &output)
	// If deployment fails, enabling, disabled, starting, running, and output cannot occur.
	err = fmt.Errorf("deployment failed for step %s/%s", r.runID, r.pluginStepID)
	r.markStageFailures(StageIDEnabling, err)
	r.markNotClosable(err)
}

func (r *runningStep) transitionToDisabled() {
	r.logger.Infof("Step %s/%s disabled", r.runID, r.pluginStepID)
	enabledOutput := any(map[any]any{"enabled": false})
	// End prior stage "enabling" with "resolved" output, and start "disabled" stage.
	r.transitionStageWithOutput(
		StageIDDisabled,
		step.RunningStepStateRunning,
		schema.PointerTo("resolved"),
		&enabledOutput,
	)
	disabledOutput := any(map[any]any{"message": fmt.Sprintf("Step %s/%s disabled", r.runID, r.pluginStepID)})
	r.completeStep(
		StageIDDisabled,
		step.RunningStepStateFinished, // Must set the stage to finished for the engine realize the step is done.
		schema.PointerTo("output"),
		&disabledOutput,
	)

	err := fmt.Errorf("step %s/%s disabled", r.runID, r.pluginStepID)
	r.markStageFailures(StageIDStarting, err)
	r.markNotClosable(err)
}

func (r *runningStep) closedEarly(stageToMarkUnresolvable StageID, priorStageFailed bool) {
	r.logger.Infof("Step %s/%s closed", r.runID, r.pluginStepID)
	// Follow the convention of transitioning to running then finished.
	if priorStageFailed {
		r.transitionFromFailedStage(StageIDClosed, step.RunningStepStateRunning, fmt.Errorf("step closed early"))
	} else {
		r.transitionRunningStage(StageIDClosed)
	}
	closedOutput := any(map[any]any{"cancelled": r.cancelled, "close_requested": r.closed.Load()})

	r.completeStep(
		StageIDClosed,
		step.RunningStepStateFinished,
		schema.PointerTo("result"),
		&closedOutput,
	)

	err := fmt.Errorf("step %s/%s closed due to workflow termination", r.runID, r.pluginStepID)
	r.markStageFailures(stageToMarkUnresolvable, err)
}

func (r *runningStep) startFailed(err error) {
	r.transitionFromFailedStage(StageIDCrashed, step.RunningStepStateRunning, err)
	r.logger.Warningf("Plugin step %s/%s start failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(Crashed{
		Output: err.Error(),
	})

	r.completeStep(StageIDCrashed, step.RunningStepStateFinished, &outputID, &output)
	r.markStageFailures(StageIDRunning, err)
	r.markNotClosable(err)
}

func (r *runningStep) runFailed(err error) {
	r.transitionRunningStage(StageIDCrashed)
	r.logger.Warningf("Plugin step %s/%s run failed. %v", r.runID, r.pluginStepID, err)

	// Now it's done.
	outputID := errorStr
	output := any(Crashed{
		Output: err.Error(),
	})
	r.completeStep(StageIDCrashed, step.RunningStepStateFinished, &outputID, &output)
	r.markStageFailures(StageIDOutput, err)
	r.markNotClosable(err)
}

// TransitionStage transitions the running step to the specified stage, and the state running.
// For other situations, use transitionFromFailedStage, completeStep, or transitionStageWithOutput
func (r *runningStep) transitionRunningStage(newStage StageID) {
	r.transitionStageWithOutput(newStage, step.RunningStepStateRunning, nil, nil)
}

func (r *runningStep) transitionFromFailedStage(newStage StageID, state step.RunningStepState, err error) {
	r.lock.Lock()
	previousStage := string(r.currentStage)
	r.currentStage = newStage
	// Don't forget to update this, or else it will behave very oddly.
	// First running, then finished. You can't skip states.
	r.state = state
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

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

var deployingLifecycleStage = step.LifecycleStage{
	ID:              "deploying",
	FinishedKeyword: "deployed",
	InputKeyword:    "deploy",
	NextStages: []string{
		"running", "deploy_failed",
	},
	Fatal: false,
}
var deployFailedLifecycleStage = step.LifecycleStage{
	ID:              "deploy_failed",
	FinishedKeyword: "deploy_failed",
	Fatal:           true,
}
var runningLifecycleStage = step.LifecycleStage{
	ID:              "running",
	FinishedKeyword: "completed",
	NextStages: []string{
		"finished", "crashed",
	},
}
var finishedLifecycleStage = step.LifecycleStage{
	ID:              "finished",
	FinishedKeyword: "finished",
}
var crashedLifecycleStage = step.LifecycleStage{
	ID:              "crashed",
	FinishedKeyword: "crashed",
}

func (p pluginProvider) Lifecycle() step.Lifecycle[step.LifecycleStage] {
	return step.Lifecycle[step.LifecycleStage]{
		InitialStage: "deploying",
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

	return &runningStep{
		stageChangeHandler: stageChangeHandler,
		stepSchema:         stepSchema,
		deployerRegistry:   r.deployerRegistry,
		stage:              "deploying",
		lock:               &sync.Mutex{},
	}, nil
}

type runningStep struct {
	deployerRegistry   registry.Registry
	stepSchema         schema.Step
	stageChangeHandler step.StageChangeHandler
	stage              string
	lock               *sync.Mutex
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
	// TODO implement me
	panic("implement me")
}

func (r runningStep) CurrentStage() string {
	return r.stage
}

# Arcaflow pluggable step system

This library provides pluggable steps to Arcaflow.

In Arcaflow, a workflow consists of one or more steps that are executed in parallel or in sequence. This library helps provide a uniform API to implement new step types. For example, the `plugin` step kind runs an Arcaflow plugin using a deployer (e.g. a container engine).

## How pluggable steps work

Each pluggable step provider goes through several stages during its life.

First, the provider does not have any configuration. In this state, the provider cannot provide any meaningful schema information other than for the provider configuration itself. For example, when the `plugin` provider is in this state, it has not yet deployed the plugin and cannot say what options the specific plugin wants.

Second, the provider can perform an action to obtain a "runnable step". In case of the `plugin` provider, it deploys the plugin using the local deployer and queries its schema. After this, the schema information is available and can be run.

Finally, the step can be run. In a running state the step goes through a set of lifecycle stages, each of which can require an input and can produce an output. For example, the `plugin` provider will, given the correct input, first be in the `configuration` stage, then advance to `deploying` and then `running`, and finally move to the `finished` stage.

## Creating your own pluggable step

### Provider

To create your own pluggable step, you must first implement the `Provider` interface described in [provider.go](provider.go). This interface describes the provider in its first phase. Here are the functions you must implement:

- `Kind()`: this function must return a globally unique value the user will have to supply in the `kind` field of their workflow.
- `Lifecycle()`: this function must return a lifecycle object structure that describes which lifecycle stages the step can go through. You won't have to provide a schema for these lifecycle stages just yet. (See lifecycles below for more details.)
- `ProviderKeyword()`: this function must return the keyword the user will have to enter in their workflow file to configure the provider. For example, the `plugin` provider uses `plugin` as a provider keyword and the user is expected to provide a plugin container image here. Be careful though, this must be unique within this provider and cannot be the same as any lifecycle keywords!
- `ProviderSchema()`: this function returns structure which describes the schema associated with the provider keyword. You must ensure that this schema matches the expected type for the `LoadSchema()` function.
- `RunKeyword()`: this function returns a keyword that holds the configuration when the step is run. For example, the `plugin` provider uses this to let users specify the `step` which to run, in case there are multiple steps provided by a plugin.
- `LoadSchema()`: this function takes the value from the provider keyword as input and loads the schema. It returns a runnable step, which already holds the schema information about the step being run.
- `Any()`: this is a helper function that should always call `AnyProvider()` to provide an untyped provider.

### Lifecycle

The lifecycle is a simple description of the stages your provider goes through. For example, the first stage of the plugin provider looks like this:

```go
var configurationLifecycleStage = step.LifecycleStage{
    // This is how the stage is called when running
    ID:              "configuring",
    // This is how the stage is called when it has finished.
    FinishedKeyword: "configured",
    // Provide the workflow keyword for configuration:
    InputKeyword:    "step",
    // These stages can possibly run next.
    NextStages: []string{
        "deploying", "configuration_failed",
    },
    // True indicates that this stage is an incurable problem in the workflow.
    Fatal: false,
}
```

You can then set up your lifecycle:

```go
var lifecycle = step.Lifecycle[step.LifecycleStage]{
    InitialStage: "configuring",
    Stages: []step.LifecycleStage{
        configurationLifecycleStage,
        // ...
    },
}
```

Later on, you will need to provide a lifecycle with schema, which you can do as follows:

```go
var lifecycleStageWithSchema = step.LifecycleStageWithSchema{
    // Embed the lifecycle from before.
    configurationLifecycleStage,
    step.NewPartialProperty[YourParameterType](
        // Add your schema here.
        mySchema,
        // Add a possible display value here.
        nil,
        // This is always required.
        true,
    ),
    map[string]*schema.StepOutputSchema{
        // Add your possible outputs here.
    }
}
```

### Runnable steps

Next, you need to create a runnable step. This must have three functions:

- `RunSchema()` provides a schema that describes the properties that are required to run the step provided by this provider. For example, the `plugin` provider uses this to specify the `step` a user should choose if the plugin provides more than one.
- `Lifecycle()` must return a lifecycle, but this time with the schema information (see above).
- `Start()` starts the step. It takes the initial input specified by `RunSchema()`, as well as a `StageChangeHandler` that will be notified if the step finishes its current stage in the lifecycle. This callback will also indicate if the step needs more data to proceed to the next step. This will allow you to incrementally feed data to the step. This function must return a running step construct as a result.

### Stage change handler

The stage change handler allows you to react to events happening in a stage:

- `OnStageChange()` is the callback that notifies the handler of the fact that the previous stage has finished with a specific output. It indicates which next stage the provider moved to, and if it is waiting for input to be provided via ProvideStageInput.
- `OnStepComplete()` is called when the step has completed a final stage in its lifecycle and communicates the output.

### Running steps

Finally, you will need to implement the running step construct. It has the following functions:

- `ProvideStageInput()` gives you the opportunity to provide input for a stage so that it may continue.
- `CurrentStage()` returns the stage the step provider is currently in, no matter if it is finished or not. 
- `Close()` shuts down the step and cleans up the resources associated with the step.
## Important things to note

The plugin provider is called from multiple goroutines and must be thread-safe. The stage and state variables MUST be consistent at all times and the provider MUST advance through the states properly (e.g. it must not skip over the "running" state.)
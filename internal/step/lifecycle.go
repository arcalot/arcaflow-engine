package step

import (
	"fmt"

	"go.arcalot.io/dgraph"
	"go.arcalot.io/lang"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Lifecycle describes the lifecycle of a step.
// Each stage in the lifecycle can, but is not required to have an input schema and an output schema.
// The stage can also declare the next possible stages.
//
// This lifecycle information is used to build the dependency tree of this step.
type Lifecycle[StageType lifecycleStage] struct {
	// InitialStage contains the first stage this step enters.
	InitialStage string
	// Stages contains the list of stages for this provider.
	Stages []StageType
}

// DAG will return a directed acyclic graph of the lifecycle.
func (l Lifecycle[StageType]) DAG() (dgraph.DirectedGraph[StageType], error) {
	dag := dgraph.New[StageType]()
	for _, stage := range l.Stages {
		_, err := dag.AddNode(stage.Identifier(), stage)
		if err != nil {
			return nil, fmt.Errorf("failed to add stage %s to lifecycle (%w)", stage.Identifier(), err)
		}
	}
	for _, stage := range l.Stages {
		node := lang.Must2(dag.GetNodeByID(stage.Identifier()))
		for _, nextStage := range stage.NextStageIDs() {
			if err := node.Connect(nextStage); err != nil {
				return nil, fmt.Errorf("failed to connect lifecycle stage %s to %s (%w)", node.ID(), nextStage, err)
			}
		}
	}
	starterNodes := dag.ListNodesWithoutInboundConnections()
	if len(starterNodes) != 1 {
		return nil, fmt.Errorf("invalid number of initial stage for lifecycle: %d", len(starterNodes))
	}
	if _, ok := starterNodes[l.InitialStage]; !ok {
		return nil, fmt.Errorf("incorrect initial stage: %s (not a stage without inbound connections", l.InitialStage)
	}
	return dag, nil
}

// lifecycleStage is a helper interface for being able to construct a DAG from a lifecycle.
type lifecycleStage interface {
	// Identifier returns the ID of the stage.
	Identifier() string
	// NextStageIDs returns the next stage identifiers.
	NextStageIDs() []string
}

// LifecycleStage is the description of a single stage within a step lifecycle.
type LifecycleStage struct {
	// ID uniquely identifies the stage within the current provider.
	ID string
	// WaitingName describes this stage when waiting for input.
	WaitingName string
	// RunningName describes this stage when it is running.
	RunningName string
	// FinishedName specifies how to call this stage once it is complete.
	FinishedName string
	// InputFields provides the set of fields containing the input data of this stage. This must match the later
	// lifecycle with schema.
	InputFields map[string]struct{}
	// NextStages describes the possible next stages. The provider advances to one of these stages automatically and
	// will pause if there is no input available.
	// It will automatically create a DAG node between the current and the described next stages to ensure
	// that it is running in order.
	NextStages []string
	// RemovedStages describes stages that should be removed from the DAG if this stage is run.
	RemovedStages []string
	// Fatal indicates that this stage should be treated as fatal unless handled by the workflow.
	Fatal bool
}

// Identifier is a helper function for getting the ID.
func (l LifecycleStage) Identifier() string {
	return l.ID
}

// NextStageIDs is a helper function that returns the next possible stages.
func (l LifecycleStage) NextStageIDs() []string {
	return l.NextStages
}

// LifecycleStageWithSchema contains information about the possible outputs of a lifecycle stage. This is available
// after the step lifecycle has started.
type LifecycleStageWithSchema struct {
	LifecycleStage

	// InputSchema describes the schema for the required input of the current stage. This may be nil if the stage
	// requires no input.
	InputSchema map[string]*schema.PropertySchema
	Outputs     map[string]*schema.StepOutputSchema
}

// Package workflow provides the workflow execution engine.
package workflow

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"go.arcalot.io/dgraph"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// executableWorkflow is an implementation of the ExecutableWorkflow interface that provides a workflow you can actually
// run.
type executableWorkflow struct {
	logger            log.Logger
	dag               dgraph.DirectedGraph[*DAGItem]
	input             schema.Scope
	stepRunData       map[string]map[string]any
	workflowContext   map[string][]byte
	internalDataModel *schema.ScopeSchema
	runnableSteps     map[string]step.RunnableStep
	lifecycles        map[string]step.Lifecycle[step.LifecycleStageWithSchema]
}

// Input returns a schema scope that can be used to validate the input.
func (e *executableWorkflow) Input() schema.Scope {
	return e.input
}

// DAG returns the constructed DAG for the workflow. You can use this to print pretty execution graphs.
func (e *executableWorkflow) DAG() dgraph.DirectedGraph[*DAGItem] {
	return e.dag
}

// Execute runs the workflow with the specified input. You can use the context variable to abort the workflow execution
// (e.g. when the user presses Ctrl+C).
func (e *executableWorkflow) Execute(ctx context.Context, input any) (outputData any, err error) {
	// First, we unserialize the input. This makes sure we didn't get garbage data.
	unserializedInput, err := e.input.Unserialize(input)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow input (%w)", err)
	}

	// We use an internal cancel function to abort the workflow if something bad happens.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	l := &loopState{
		logger: e.logger,
		lock:   &sync.Mutex{},
		data: map[string]any{
			"input": unserializedInput,
			"steps": map[string]any{},
		},
		dag:               e.dag.Clone(),
		inputsNotified:    make(map[string]struct{}, len(e.dag.ListNodes())),
		runningSteps:      make(map[string]step.RunningStep, len(e.dag.ListNodes())),
		outputDataChannel: make(chan any, 1),
		outputDone:        false,
		cancel:            cancel,
		workflowContext:   e.workflowContext,
	}

	for stepID, runnableStep := range e.runnableSteps {
		stepID := stepID
		runnableStep := runnableStep
		stepDataModel := map[string]any{}
		for _, stage := range e.lifecycles[stepID].Stages {
			steps := l.data["steps"].(map[string]any)
			if _, ok := steps[stepID]; !ok {
				steps[stepID] = map[string]any{}
			}
			stages := steps[stepID].(map[string]any)
			stages[stage.ID] = map[string]any{}
		}
		l.data["steps"].(map[string]any)[stepID] = stepDataModel

		var stageHandler step.StageChangeHandler = &stageChangeHandler{
			onStageChange: func(step step.RunningStep, previousStage *string, previousStageOutputID *string, previousStageOutput *any, stage string, waitingForInput bool) {
				waitingForInputText := ""
				if waitingForInput {
					waitingForInputText = " and is waiting for input"
				}
				e.logger.Debugf("Stage change for step %s to %s%s...", stepID, stage, waitingForInputText)
				l.onStageComplete(stepID, previousStage, previousStageOutputID, previousStageOutput)
			},
			onStepComplete: func(step step.RunningStep, previousStage string, previousStageOutputID *string, previousStageOutput *any) {
				if previousStageOutputID != nil {
					e.logger.Debugf("Step %s completed with stage '%s', output '%s'...", stepID, previousStage, *previousStageOutputID)
				} else {
					e.logger.Debugf("Step %s completed with stage '%s'...", stepID, previousStage)
				}
				l.onStageComplete(stepID, &previousStage, previousStageOutputID, previousStageOutput)
			},
		}
		e.logger.Debugf("Launching step %s...", stepID)
		runningStep, err := runnableStep.Start(e.stepRunData[stepID], stageHandler)
		if err != nil {
			return nil, fmt.Errorf("failed to launch step %s (%w)", stepID, err)
		}
		l.runningSteps[stepID] = runningStep
	}
	// Let's make sure we are closing all steps once this function terminates so we don't leave stuff running.
	defer func() {
		e.logger.Debugf("Terminating all steps...")
		for stepID, runningStep := range l.runningSteps {
			e.logger.Debugf("Terminating step %s...", stepID)
			if err := runningStep.Close(); err != nil {
				panic(fmt.Errorf("failed to close step %s (%w)", stepID, err))
			}
		}
	}()

	// We remove the input node from the DAG and call the notifySteps function once to trigger the workflow
	// start.
	e.logger.Debugf("Starting workflow execution...\n%s", l.dag.Mermaid())
	inputNode, err := l.dag.GetNodeByID("input")
	if err != nil {
		return nil, fmt.Errorf("bug: cannot obtain input node (%w)", err)
	}
	if err := inputNode.Remove(); err != nil {
		return nil, fmt.Errorf("failed to remove input node from DAG (%w)", err)
	}

	func() {
		// Defer to ensure that if it crashes, it properly unlocks
		// This is to prevent a deadlock.
		l.lock.Lock()
		defer l.lock.Unlock()
		l.notifySteps()
	}()

	// Now we wait for the workflow results.
	select {
	case outputData, ok := <-l.outputDataChannel:
		if !ok {
			return nil, fmt.Errorf("output data channel unexpectedly closed")
		}
		e.logger.Debugf("Output complete.")
		return outputData, nil
	case <-ctx.Done():
		e.logger.Debugf("Workflow execution aborted. %s", l.lastError)
		if l.lastError != nil {
			return nil, l.lastError
		}
		return nil, fmt.Errorf("workflow execution aborted (%w)", ctx.Err())
	}
}

type loopState struct {
	logger            log.Logger
	lock              *sync.Mutex
	data              map[string]any
	dag               dgraph.DirectedGraph[*DAGItem]
	inputsNotified    map[string]struct{}
	runningSteps      map[string]step.RunningStep
	outputDataChannel chan any
	outputDone        bool
	lastError         error
	cancel            context.CancelFunc
	workflowContext   map[string][]byte
}

func (l *loopState) onStageComplete(stepID string, previousStage *string, previousStageOutputID *string, previousStageOutput *any) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if previousStage == nil {
		return
	}
	stageNode, err := l.dag.GetNodeByID(GetStageNodeID(stepID, *previousStage))
	if err != nil {
		l.logger.Errorf("Failed to get stage node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
		l.lastError = fmt.Errorf("failed to get stage node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
		l.cancel()
		return
	}
	if err := stageNode.Remove(); err != nil {
		l.logger.Errorf("Failed to remove stage node ID %s (%w)", stageNode.ID(), err)
		l.lastError = fmt.Errorf("failed to remove stage node ID %s (%w)", stageNode.ID(), err)
		l.cancel()
		return
	}
	if previousStageOutputID != nil {
		outputNode, err := l.dag.GetNodeByID(GetOutputNodeID(stepID, *previousStage, *previousStageOutputID))
		if err != nil {
			l.logger.Errorf("Failed to get output node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
			l.lastError = fmt.Errorf("failed to get output node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
			l.cancel()
			return
		}
		if err := outputNode.Remove(); err != nil {
			l.logger.Errorf("Failed to remove output node ID %s (%w)", outputNode.ID(), err)
			l.lastError = fmt.Errorf("failed to remove output node ID %s (%w)", outputNode.ID(), err)
			l.cancel()
			return
		}
		// Placing data from the output into the general data structure
		l.data["steps"].(map[string]any)[stepID].(map[string]any)[*previousStage] = map[string]any{}
		l.data["steps"].(map[string]any)[stepID].(map[string]any)[*previousStage].(map[string]any)[*previousStageOutputID] = *previousStageOutput
	}
	l.notifySteps()
}

// notifySteps is a function we can call to go through all DAG nodes that have no inbound connections and
// provide step inputs based on expressions.
func (l *loopState) notifySteps() { //nolint:gocognit
	// This function goes through the DAG and feeds the input to all steps that have no further inbound
	// dependencies.
	//
	// This function could be further optimized if there was a DAG that contained not only the steps, but the
	// concrete values that needed to be updated. This would make it possible to completely forego the need to
	// iterate through the input.

	nodesWithoutInbound := l.dag.ListNodesWithoutInboundConnections()
	l.logger.Debugf("Currently %d DAG nodes have no inbound connection. Now processing them.", len(nodesWithoutInbound))
	for nodeID, node := range nodesWithoutInbound {
		l.logger.Debugf("Processing step node %s", nodeID)
		if _, ok := l.inputsNotified[nodeID]; ok {
			continue
		}
		l.inputsNotified[nodeID] = struct{}{}
		inputData := node.Item().Input
		if inputData == nil {
			// No input data is needed.
			continue
		}
		// Resolve any expressions in the input data.
		untypedInputData, err := l.resolveExpressions(inputData, l.data)
		if err != nil {
			panic(fmt.Errorf("cannot resolve expressions for %s (%w)", nodeID, err))
		}

		switch node.Item().Kind {
		case DAGItemKindStepStage:
			if node.Item().InputSchema == nil {
				break
			}
			// We have a stage we can proceed with. Let's provide it with input.
			if _, err := node.Item().InputSchema.Unserialize(untypedInputData); err != nil {
				l.logger.Errorf("Bug: schema evaluation resulted in invalid data for %s (%v)", node.ID(), err)
				l.lastError = fmt.Errorf("bug: schema evaluation resulted in invalid data for %s (%w)", node.ID(), err)
				l.cancel()
				return
			}

			if node.Item().StepID != "" && node.Item().StageID != "" {
				stageInputData := untypedInputData.(map[any]any)
				typedInputData := make(map[string]any, len(stageInputData))
				for k, v := range stageInputData {
					typedInputData[k.(string)] = v
				}
				l.logger.Debugf("Providing stage input for %s...", nodeID)
				if err := l.runningSteps[node.Item().StepID].ProvideStageInput(
					node.Item().StageID,
					typedInputData,
				); err != nil {
					l.logger.Errorf("Bug: failed to provide input to step %s (%w)", node.Item().StepID, err)
					l.lastError = fmt.Errorf("bug: failed to provide input to step %s (%w)", node.Item().StepID, err)
					l.cancel()
					return
				}
			}
		case DAGItemKindOutput:
			// We have received enough data to construct the workflow output.
			l.logger.Debugf("Constructing workflow output.")
			l.outputDone = true
			l.outputDataChannel <- untypedInputData

			if err := node.Remove(); err != nil {
				l.logger.Errorf("BUG: Error occurred while removing workflow output node (%w)", err)
			}
		}
	}
	// Here we make sure we don't have a deadlock.
	counters := struct {
		starting int
		waiting  int
		running  int
		finished int
	}{
		0,
		0,
		0,
		0,
	}
	for stepID, runningStep := range l.runningSteps {
		switch runningStep.State() {
		case step.RunningStepStateStarting:
			counters.starting++
			l.logger.Debugf("Step %s is currently starting.", stepID)
		case step.RunningStepStateWaitingForInput:
			counters.waiting++

			connectionsMsg := ""
			dagNode, err := l.dag.GetNodeByID(GetStageNodeID(stepID, runningStep.CurrentStage()))
			if err != nil {
				l.logger.Warningf("Failed to get DAG node for the debug message (%w)", err)
			} else if dagNode == nil {
				l.logger.Warningf("Failed to get DAG node for the debug message. Returned nil", err)
			} else {
				inboundConnections, err := dagNode.ListInboundConnections()
				if err != nil {
					l.logger.Warningf("Error while listing inbound connections. (%w)", err)
				}

				i := 0
				for k := range inboundConnections {
					if i > 0 {
						connectionsMsg += ", "
					}
					connectionsMsg += k
					i++
				}
			}

			l.logger.Debugf("Step %s, stage %s, is currently waiting for input from '%s'.", stepID, runningStep.CurrentStage(), connectionsMsg)
		case step.RunningStepStateRunning:
			counters.running++
			l.logger.Debugf("Step %s is currently running.", stepID)
		case step.RunningStepStateFinished:
			counters.finished++
			l.logger.Debugf("Step %s is currently finished.", stepID)
		}
	}
	l.logger.Infof(
		"There are currently %d steps starting, %d waiting for input, %d running, %d finished",
		counters.starting,
		counters.waiting,
		counters.running,
		counters.finished,
	)
	if counters.starting == 0 && counters.running == 0 && !l.outputDone {
		outputNode, err := l.dag.GetNodeByID("output")
		if err != nil {
			panic(fmt.Errorf("cannot fetch output node (%w)", err))
		}
		var unmetDependencies []string
		inbound, err := outputNode.ListInboundConnections()
		if err != nil {
			panic(fmt.Errorf("failed to fetch output node inbound dependencies (%w)", err))
		}
		for i := range inbound {
			unmetDependencies = append(unmetDependencies, i)
		}
		l.logger.Errorf("No steps running, no more executable steps, cannot construct output (has the following unmet dependencies: %s)", strings.Join(unmetDependencies, ", "))
		l.logger.Debugf("DAG:\n%s", l.dag.Mermaid())
		l.lastError = fmt.Errorf("no steps running, no more executable steps, cannot construct output (has the following unment dependencies: %s)", strings.Join(unmetDependencies, ", "))
		l.cancel()
	}

}

// resolveExpressions takes an inputData value potentially containing expressions and a dataModel containing data
// for expressions and resolves the expressions contained in inputData using reflection.
func (l *loopState) resolveExpressions(inputData any, dataModel any) (any, error) {
	if expr, ok := inputData.(expressions.Expression); ok {
		l.logger.Debugf("Evaluating expression %s...", expr.String())
		return expr.Evaluate(dataModel, l.workflowContext)
	}
	v := reflect.ValueOf(inputData)
	switch v.Kind() {
	case reflect.Slice:
		result := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			value := v.Index(i).Interface()
			newValue, err := l.resolveExpressions(value, dataModel)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve expressions (%w)", err)
			}
			result[i] = newValue
		}
		return result, nil
	case reflect.Map:
		result := make(map[any]any, v.Len())
		for _, reflectedKey := range v.MapKeys() {
			key := reflectedKey.Interface()
			value := v.MapIndex(reflectedKey).Interface()
			newValue, err := l.resolveExpressions(value, dataModel)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve expressions (%w)", err)
			}
			result[key] = newValue
		}
		return result, nil
	default:
		return inputData, nil
	}
}

type stageChangeHandler struct {
	onStageChange  func(step step.RunningStep, previousStage *string, previousStageOutputID *string, previousStageOutput *any, stage string, waitingForInput bool)
	onStepComplete func(step step.RunningStep, previousStage string, previousStageOutputID *string, previousStageOutput *any)
}

func (s stageChangeHandler) OnStageChange(step step.RunningStep, previousStage *string, previousStageOutputID *string, previousStageOutput *any, stage string, waitingForInput bool) {
	s.onStageChange(step, previousStage, previousStageOutputID, previousStageOutput, stage, waitingForInput)
}

func (s stageChangeHandler) OnStepComplete(step step.RunningStep, previousStage string, previousStageOutputID *string, previousStageOutput *any) {
	s.onStepComplete(step, previousStage, previousStageOutputID, previousStageOutput)
}

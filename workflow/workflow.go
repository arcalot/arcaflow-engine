// Package workflow provides the workflow execution engine.
package workflow

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.flow.arcalot.io/engine/config"

	"go.arcalot.io/dgraph"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/step"
	"go.flow.arcalot.io/expressions"
	"go.flow.arcalot.io/pluginsdk/schema"
)

const (
	// WorkflowInputKey is the key in the workflow map for input.
	WorkflowInputKey = "input"
	// WorkflowStepsKey is the key in the workflow map for the steps.
	WorkflowStepsKey = "steps"
)

// executableWorkflow is an implementation of the ExecutableWorkflow interface that provides a workflow you can actually
// run.
type executableWorkflow struct {
	logger            log.Logger
	config            *config.Config
	dag               dgraph.DirectedGraph[*DAGItem]
	input             schema.Scope
	stepRunData       map[string]map[string]any
	workflowContext   map[string][]byte
	internalDataModel *schema.ScopeSchema
	runnableSteps     map[string]step.RunnableStep
	lifecycles        map[string]step.Lifecycle[step.LifecycleStageWithSchema]
	outputSchema      map[string]*schema.StepOutputSchema
}

func (e *executableWorkflow) OutputSchema() map[string]*schema.StepOutputSchema {
	return e.outputSchema
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
func (e *executableWorkflow) Execute(ctx context.Context, serializedInput any) (outputID string, outputData any, err error) { //nolint:gocognit
	// First, we unserialize the input. This makes sure we didn't get garbage data.

	unserializedInput, err := e.input.Unserialize(serializedInput)
	if err != nil {
		return "", nil, fmt.Errorf("invalid workflow input (%w)", err)
	}
	reSerializedInput, err := e.input.Serialize(unserializedInput)
	if err != nil {
		return "", nil, fmt.Errorf("failed to reserialize workflow input (%w)", err)
	}
	err = e.input.Validate(reSerializedInput)
	if err != nil {
		return "", nil, fmt.Errorf("bug: reserialized data is invalid %v", reSerializedInput)
	}

	// We use an internal cancel function to abort the workflow if something bad happens.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	l := &loopState{
		logger: e.logger.WithLabel("source", "workflow"),
		config: e.config,
		lock:   &sync.Mutex{},
		data: map[string]any{
			WorkflowInputKey: reSerializedInput,
			WorkflowStepsKey: map[string]any{},
		},
		dag:               e.dag.Clone(),
		inputsNotified:    make(map[string]struct{}, len(e.dag.ListNodes())),
		runningSteps:      make(map[string]step.RunningStep, len(e.dag.ListNodes())),
		outputDataChannel: make(chan outputDataType, 1),
		outputDone:        false,
		cancel:            cancel,
		workflowContext:   e.workflowContext,
		recentErrors:      make(chan error, 20), // Big buffer in case there are lots of subsequent errors.
	}

	l.lock.Lock()

	// Iterate over all steps to set them up with proper handlers, then launch them.
	// Even though they're launched, the workflow won't execute until the input is provided.
	for stepID, runnableStep := range e.runnableSteps {
		stepID := stepID
		runnableStep := runnableStep
		stepDataModel := map[string]any{}
		for _, stage := range e.lifecycles[stepID].Stages {
			steps := l.data[WorkflowStepsKey].(map[string]any)
			if _, ok := steps[stepID]; !ok {
				steps[stepID] = map[string]any{}
			}
			stages := steps[stepID].(map[string]any)
			stages[stage.ID] = map[string]any{}
		}
		l.data[WorkflowStepsKey].(map[string]any)[stepID] = stepDataModel

		var stageHandler step.StageChangeHandler = &stageChangeHandler{
			onStageChange: func(
				step step.RunningStep,
				previousStage *string,
				previousStageOutputID *string,
				previousStageOutput *any,
				stage string,
				inputAvailable bool,
				wg *sync.WaitGroup,
			) {
				waitingForInputText := ""
				if !inputAvailable {
					waitingForInputText = " and is waiting for input"
				}
				e.logger.Debugf("START Stage change for step %s to %s%s...", stepID, stage, waitingForInputText)
				l.onStageComplete(stepID, previousStage, previousStageOutputID, previousStageOutput, wg)
				e.logger.Debugf("DONE Stage change for step %s to %s%s...", stepID, stage, waitingForInputText)
			},
			onStepComplete: func(
				step step.RunningStep,
				previousStage string,
				previousStageOutputID *string,
				previousStageOutput *any,
				wg *sync.WaitGroup,
			) {
				if previousStageOutputID != nil {
					e.logger.Debugf("Step %s completed with stage '%s', output '%s'...", stepID, previousStage, *previousStageOutputID)
				} else {
					e.logger.Debugf("Step %s completed with stage '%s'...", stepID, previousStage)
				}
				l.onStageComplete(stepID, &previousStage, previousStageOutputID, previousStageOutput, wg)
			},
		}
		e.logger.Debugf("Launching step %s...", stepID)
		runningStep, err := runnableStep.Start(e.stepRunData[stepID], stepID, stageHandler)
		if err != nil {
			return "", nil, fmt.Errorf("failed to launch step %s (%w)", stepID, err)
		}
		l.runningSteps[stepID] = runningStep
	}
	l.lock.Unlock()
	// Let's make sure we are closing all steps once this function terminates so we don't leave stuff running.
	defer func() {
		e.logger.Debugf("Terminating all steps...")
		for stepID, runningStep := range l.runningSteps {
			e.logger.Debugf("Terminating step %s...", stepID)
			if err := runningStep.ForceClose(); err != nil {
				panic(fmt.Errorf("failed to close step %s (%w)", stepID, err))
			}
		}
	}()

	// We remove the input node from the DAG and call the notifySteps function once to trigger the workflow
	// start.
	e.logger.Debugf("Starting workflow execution...\n%s", l.dag.Mermaid())
	inputNode, err := l.dag.GetNodeByID(WorkflowInputKey)
	if err != nil {
		return "", nil, fmt.Errorf("bug: cannot obtain input node (%w)", err)
	}
	if err := inputNode.Remove(); err != nil {
		return "", nil, fmt.Errorf("failed to remove input node from DAG (%w)", err)
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
	case outputDataEntry, ok := <-l.outputDataChannel:
		if !ok {
			return "", nil, fmt.Errorf("output data channel unexpectedly closed")
		}
		e.logger.Debugf("Output complete.")
		outputID = outputDataEntry.outputID
		outputData = outputDataEntry.outputData
		outputSchema, ok := e.outputSchema[outputID]
		if !ok {
			return "", nil, fmt.Errorf(
				"bug: no output named '%s' found in output schema",
				outputID,
			)
		}
		_, err := outputSchema.Unserialize(outputDataEntry.outputData)
		if err != nil {
			return "", nil, fmt.Errorf(
				"bug: output schema cannot unserialize output data (%w)",
				err,
			)
		}
		return outputDataEntry.outputID, outputData, nil
	case <-ctx.Done():
		lastErr := l.getLastError()
		e.logger.Debugf("Workflow execution aborted. %s", lastErr)
		if lastErr != nil {
			return "", nil, lastErr
		}
		return "", nil, fmt.Errorf("workflow execution aborted (%w)", ctx.Err())
	}
}

type outputDataType struct {
	outputID   string
	outputData any
}

type loopState struct {
	logger log.Logger
	config *config.Config
	lock   *sync.Mutex
	// data holds the whole data model.
	// It is tempting to touch this to get data for things, but don't touch it.
	// If steps use this, it could cause dependency problems, concurrency problems, and scalability problems.
	data              map[string]any
	dag               dgraph.DirectedGraph[*DAGItem]
	inputsNotified    map[string]struct{}
	runningSteps      map[string]step.RunningStep
	outputDataChannel chan outputDataType
	outputDone        bool
	recentErrors      chan error
	cancel            context.CancelFunc
	workflowContext   map[string][]byte
}

// getLastError gathers the last errors. If there are several, it creates a new one that consolidates them.
// This will read from the channel. Calling again will only gather new errors since the last call.
func (l *loopState) getLastError() error {
	var errors []error
errGatherLoop:
	for {
		select {
		case err := <-l.recentErrors:
			errors = append(errors, err)
		default:
			break errGatherLoop // No more errors
		}
	}
	switch len(errors) {
	case 0:
		return nil
	case 1:
		return errors[0]
	default:
		return fmt.Errorf("multiple errors: %v", errors)
	}
}

func (l *loopState) onStageComplete(stepID string, previousStage *string, previousStageOutputID *string, previousStageOutput *any, wg *sync.WaitGroup) {
	l.lock.Lock()
	defer func() {
		if previousStage != nil {
			l.checkForDeadlocks(3, wg)
		}
		l.lock.Unlock()
	}()

	if previousStage == nil {
		return
	}
	stageNode, err := l.dag.GetNodeByID(GetStageNodeID(stepID, *previousStage))
	if err != nil {
		l.logger.Errorf("Failed to get stage node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
		l.recentErrors <- fmt.Errorf("failed to get stage node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
		l.cancel()
		return
	}
	l.logger.Debugf("Removed node '%s' from the DAG", stageNode.ID())
	if err := stageNode.Remove(); err != nil {
		l.logger.Errorf("Failed to remove stage node ID %s (%w)", stageNode.ID(), err)
		l.recentErrors <- fmt.Errorf("failed to remove stage node ID %s (%w)", stageNode.ID(), err)
		l.cancel()
		return
	}
	if previousStageOutputID != nil {
		outputNode, err := l.dag.GetNodeByID(GetOutputNodeID(stepID, *previousStage, *previousStageOutputID))
		if err != nil {
			l.logger.Errorf("Failed to get output node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
			l.recentErrors <- fmt.Errorf("failed to get output node ID %s (%w)", GetStageNodeID(stepID, *previousStage), err)
			l.cancel()
			return
		}
		// Removes the node from the DAG. This results in the nodes not having inbound connections, allowing them to be processed.
		l.logger.Debugf("Removed node '%s' from the DAG", outputNode.ID())
		if err := outputNode.Remove(); err != nil {
			l.logger.Errorf("Failed to remove output node ID %s (%w)", outputNode.ID(), err)
			l.recentErrors <- fmt.Errorf("failed to remove output node ID %s (%w)", outputNode.ID(), err)
			l.cancel()
			return
		}
		// Handle custom logging
		stepLogConfig := l.config.LoggedOutputConfigs[*previousStageOutputID]
		if stepLogConfig != nil {
			l.logger.Writef(
				stepLogConfig.LogLevel,
				"Output ID for step \"%s\" is \"%s\".\nOutput data: \"%s\"",
				stepID,
				*previousStageOutputID,
				*previousStageOutput,
			)
		}

		// Placing data from the output into the general data structure
		l.data[WorkflowStepsKey].(map[string]any)[stepID].(map[string]any)[*previousStage] = map[string]any{}
		l.data[WorkflowStepsKey].(map[string]any)[stepID].(map[string]any)[*previousStage].(map[string]any)[*previousStageOutputID] = *previousStageOutput
	}
	l.notifySteps()
}

// notifySteps is a function we can call to go through all DAG nodes that have no inbound connections and
// provide step inputs based on expressions.
// The lock should be acquired by the caller before this is called.
func (l *loopState) notifySteps() { //nolint:gocognit
	// This function goes through the DAG and feeds the input to all steps that have no further inbound
	// dependencies.
	//
	// This function could be further optimized if there was a DAG that contained not only the steps, but the
	// concrete values that needed to be updated. This would make it possible to completely forego the need to
	// iterate through the input.

	nodesWithoutInbound := l.dag.ListNodesWithoutInboundConnections()
	l.logger.Debugf("Currently %d DAG nodes have no inbound connection. Now processing them.", len(nodesWithoutInbound))

	// nodesWithoutInbound have all dependencies resolved. No inbound connection.
	// Also includes nodes that are not for running, like an input.
	for nodeID, node := range nodesWithoutInbound {
		if _, ok := l.inputsNotified[nodeID]; ok {
			continue
		}
		l.logger.Debugf("Processing step node %s", nodeID)
		l.inputsNotified[nodeID] = struct{}{}
		// The data structure that the particular node requires. One or more fields. May or may not contain expressions.
		inputData := node.Item().Data
		if inputData == nil {
			// No input data is needed.
			continue
		}
		// Resolve any expressions in the input data.
		// untypedInputData stores the resolved data
		untypedInputData, err := l.resolveExpressions(inputData, l.data)
		if err != nil {
			panic(fmt.Errorf("cannot resolve expressions for %s (%w)", nodeID, err))
		}

		// This switch checks to see if it's a node that needs to be run.
		switch node.Item().Kind {
		case DAGItemKindStepStage:
			if node.Item().DataSchema == nil {
				// This should only happen if the stage doesn't have any input fields.
				// This may not even get called. That should be checked.
				break
			}
			// We have a stage we can proceed with. Let's provide it with input.
			// Tries to match the schema
			if _, err := node.Item().DataSchema.Unserialize(untypedInputData); err != nil {
				l.logger.Errorf("Bug: schema evaluation resulted in invalid data for %s (%v)", node.ID(), err)
				l.recentErrors <- fmt.Errorf("bug: schema evaluation resulted in invalid data for %s (%w)", node.ID(), err)
				l.cancel()
				return
			}

			// This check is here just to make sure it has the required fields set
			if node.Item().StepID == "" || node.Item().StageID == "" {
				// This shouldn't happen
				panic("Step or stage ID missing")
			}

			stageInputData := untypedInputData.(map[any]any)
			typedInputData := make(map[string]any, len(stageInputData))
			for k, v := range stageInputData {
				typedInputData[k.(string)] = v
			}
			// Sends it to the plugin
			l.logger.Debugf("Providing stage input for %s...", nodeID)
			if err := l.runningSteps[node.Item().StepID].ProvideStageInput(
				node.Item().StageID,
				typedInputData,
			); err != nil {
				l.logger.Errorf("Bug: failed to provide input to step %s (%w)", node.Item().StepID, err)
				l.recentErrors <- fmt.Errorf("bug: failed to provide input to step %s (%w)", node.Item().StepID, err)
				l.cancel()
				return
			}
		case DAGItemKindOutput:
			// We have received enough data to construct the workflow output.
			l.logger.Debugf("Constructing workflow output.")
			if l.outputDone {
				l.logger.Warningf("Workflow already done. Skipping output.")
			} else {
				l.outputDone = true

				// When this function is called, the lock should be acquired, so no
				// other copies of this should be attempting to write to the output
				// data channel. This is required to prevent the goroutine from stalling.
				l.outputDataChannel <- outputDataType{
					outputID:   node.Item().OutputID,
					outputData: untypedInputData,
				}
				// Since this is the only thread accessing the channel, it should be
				// safe to close it now
				close(l.outputDataChannel)
			}

			if err := node.Remove(); err != nil {
				l.logger.Errorf("BUG: Error occurred while removing workflow output node (%w)", err)
			}
		}
	}
}

type stateCounters struct {
	starting              int
	waitingWithInbound    int
	waitingWithoutInbound int
	running               int
	finished              int
}

func (l *loopState) countStates() stateCounters {
	counters := struct {
		starting              int
		waitingWithInbound    int
		waitingWithoutInbound int
		running               int
		finished              int
	}{
		0,
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
			connectionsMsg := ""
			dagNode, err := l.dag.GetNodeByID(GetStageNodeID(stepID, runningStep.CurrentStage()))
			switch {
			case err != nil:
				l.logger.Warningf("Failed to get DAG node for the debug message (%w)", err)
				counters.waitingWithInbound++
			case dagNode == nil:
				l.logger.Warningf("Failed to get DAG node for the debug message. Returned nil", err)
				counters.waitingWithInbound++
			default:
				inboundConnections, err := dagNode.ListInboundConnections()
				if err != nil {
					l.logger.Warningf("Error while listing inbound connections. (%w)", err)
				}
				if len(inboundConnections) > 0 {
					counters.waitingWithInbound++
				} else {
					counters.waitingWithoutInbound++
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
	return counters
}

func (l *loopState) checkForDeadlocks(retries int, wg *sync.WaitGroup) {
	// Here we make sure we don't have a deadlock.
	counters := l.countStates()
	l.logger.Infof(
		"There are currently %d steps starting, %d waiting for input, %d ready for input, %d running, %d finished",
		counters.starting,
		counters.waitingWithInbound,
		counters.waitingWithoutInbound,
		counters.running,
		counters.finished,
	)
	if counters.starting == 0 && counters.running == 0 && counters.waitingWithoutInbound == 0 && !l.outputDone {
		if retries <= 0 {
			l.recentErrors <- &ErrNoMorePossibleSteps{
				l.dag,
			}
			l.logger.Debugf("DAG:\n%s", l.dag.Mermaid())
			l.cancel()
		} else {
			// Retry. There are times when all the steps are in a transition state.
			// Retrying will delay the check until after they are done with the transition.
			l.logger.Warningf("No running steps. Rechecking...")
			wg.Add(1)
			go func() {
				time.Sleep(5 * time.Millisecond)
				l.checkForDeadlocks(retries-1, wg)
				wg.Done()
			}()
		}
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
				return nil, fmt.Errorf("failed to resolve workflow slice expressions (%w)", err)
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
				return nil, fmt.Errorf("failed to resolve workflow map expressions (%w)", err)
			}
			result[key] = newValue
		}
		return result, nil
	default:
		return inputData, nil
	}
}

// stageChangeHandler is implementing step.StageChangeHandler.
type stageChangeHandler struct {
	onStageChange func(
		step step.RunningStep,
		previousStage *string,
		previousStageOutputID *string,
		previousStageOutput *any,
		stage string,
		waitingForInput bool,
		wg *sync.WaitGroup,
	)
	onStepComplete func(
		step step.RunningStep,
		previousStage string,
		previousStageOutputID *string,
		previousStageOutput *any,
		wg *sync.WaitGroup,
	)
}

func (s stageChangeHandler) OnStageChange(
	step step.RunningStep,
	previousStage *string,
	previousStageOutputID *string,
	previousStageOutput *any,
	stage string,
	waitingForInput bool,
	wg *sync.WaitGroup,
) {
	s.onStageChange(step, previousStage, previousStageOutputID, previousStageOutput, stage, waitingForInput, wg)
}

func (s stageChangeHandler) OnStepComplete(
	step step.RunningStep,
	previousStage string,
	previousStageOutputID *string,
	previousStageOutput *any,
	wg *sync.WaitGroup,
) {
	s.onStepComplete(step, previousStage, previousStageOutputID, previousStageOutput, wg)
}

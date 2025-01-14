// Package workflow provides the workflow execution engine.
package workflow

import (
	"context"
	"fmt"
	"go.flow.arcalot.io/engine/internal/infer"
	"go.flow.arcalot.io/engine/internal/tablefmt"
	"go.flow.arcalot.io/engine/internal/tableprinter"
	"io"
	"reflect"
	"strings"
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
	callableFunctions map[string]schema.CallableFunction
	dag               dgraph.DirectedGraph[*DAGItem]
	input             schema.Scope
	stepRunData       map[string]map[string]any
	workflowContext   map[string][]byte
	internalDataModel *schema.ScopeSchema
	// All of these fields have the step ID as the key.
	runnableSteps map[string]step.RunnableStep
	lifecycles    map[string]step.Lifecycle[step.LifecycleStageWithSchema]
	outputSchema  map[string]*schema.StepOutputSchema
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

// Namespaces returns a namespaced collection of objects for the inputs
// and outputs of each stage in the step's lifecycles.
// It maps namespace id (path) to object id to object schema.
func (e *executableWorkflow) Namespaces() map[string]map[string]*schema.ObjectSchema {
	return BuildNamespaces(e.lifecycles)
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

	// We use an internal cancel function to abort the workflow if something bad happens.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outputNodes := make(map[string]dgraph.Node[*DAGItem])
	for _, node := range e.dag.ListNodes() {
		if node.Item().Kind == DAGItemKindOutput {
			outputNodes[node.ID()] = node
		}
	}

	l := &loopState{
		logger: e.logger.WithLabel("source", "workflow"),
		config: e.config,
		lock:   &sync.Mutex{},
		data: map[string]any{
			WorkflowInputKey: reSerializedInput,
			WorkflowStepsKey: map[string]any{},
		},
		callableFunctions: e.callableFunctions,
		dag:               e.dag.Clone(),
		runningSteps:      make(map[string]step.RunningStep, len(e.dag.ListNodes())),
		outputDataChannel: make(chan outputDataType, 1),
		outputDone:        false,
		waitingOutputs:    outputNodes,
		context:           ctx,
		cancel:            cancel,
		workflowContext:   e.workflowContext,
		recentErrors:      make(chan error, 20), // Big buffer in case there are lots of subsequent errors.
		lifecycles:        e.lifecycles,
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
				_ step.RunningStep,
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
				e.logger.Debugf("Stage change for step %s to %s%s...", stepID, stage, waitingForInputText)
				l.onStageComplete(stepID, previousStage, previousStageOutputID, previousStageOutput, wg)
			},
			onStepComplete: func(
				_ step.RunningStep,
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
			onStepStageFailure: func(_ step.RunningStep, stage string, _ *sync.WaitGroup, err error) {
				if err == nil {
					e.logger.Debugf("Step %q stage %q declared that it will not produce an output", stepID, stage)
				} else {
					e.logger.Debugf("Step %q stage %q declared that it will not produce an output (%s)", stepID, stage, err.Error())
				}
				l.lock.Lock()
				defer l.lock.Unlock()
				l.markOutputsUnresolvable(stepID, stage, nil)
				l.markStageNodeUnresolvable(stepID, stage)
				l.notifySteps()
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
	defer l.terminateAllSteps()

	// We remove the input node from the DAG and call the notifySteps function once to trigger the workflow
	// start.
	e.logger.Debugf("Starting workflow execution...\n%s", l.dag.Mermaid())
	inputNode, err := l.dag.GetNodeByID(WorkflowInputKey)
	if err != nil {
		return "", nil, fmt.Errorf("bug: cannot obtain input node (%w)", err)
	}
	if err := l.dag.PushStartingNodes(); err != nil {
		return "", nil, fmt.Errorf("failed to setup starting nodes in DAG (%w)", err)
	}
	if err := inputNode.ResolveNode(dgraph.Resolved); err != nil {
		return "", nil, fmt.Errorf("failed to resolve input node in DAG (%w)", err)
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
			lastErrors := l.handleErrors()
			return "", nil,
				fmt.Errorf("output data channel unexpectedly closed. %w", lastErrors)
		}
		return e.handleOutput(l, outputDataEntry)
	case <-ctx.Done():
		lastErrors := l.handleErrors()
		if lastErrors == nil {
			e.logger.Warningf(
				"Workflow execution aborted. Waiting for output before terminating (%w)",
				lastErrors)
		} else {
			e.logger.Debugf("Workflow execution exited with error after context done")
			return "", nil, lastErrors
		}
		timedContext, cancelFunction := context.WithTimeout(context.Background(), 5*time.Second)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.terminateAllSteps()
		}()
		defer wg.Wait()
		defer cancelFunction()
		select {
		case outputDataEntry, ok := <-l.outputDataChannel:
			if !ok {
				lastErrors := l.handleErrors()
				return "", nil,
					fmt.Errorf(
						"output data channel unexpectedly closed while waiting after execution aborted (%w)",
						lastErrors)
			}
			return e.handleOutput(l, outputDataEntry)
		case err := <-l.recentErrors: // The context is done, so instead just check for errors.
			// Put it back in the channel
			l.recentErrors <- err
			lastErrors := l.handleErrors()
			l.logger.Errorf("workflow failed with error %s", err.Error())
			return "", nil, lastErrors
		case <-timedContext.Done():
			lastErrors := l.handleErrors()
			var errMsg string
			if lastErrors == nil {
				errMsg = ""
			} else {
				errMsg = lastErrors.Error()
			}
			return "", nil, fmt.Errorf("workflow execution aborted (%w) (%s)", ctx.Err(), errMsg)
		}

	}
}

func (e *executableWorkflow) handleOutput(l *loopState, outputDataEntry outputDataType) (outputID string, outputData any, err error) {
	lastErrors := l.handleErrors()
	if lastErrors != nil {
		e.logger.Warningf("output completed with errors (%s)", lastErrors.Error())
	} else {
		e.logger.Debugf("output complete with output ID %s", outputDataEntry.outputID)
	}
	outputID = outputDataEntry.outputID
	outputData = outputDataEntry.outputData
	outputSchema, ok := e.outputSchema[outputID]
	if !ok {
		return "", nil, fmt.Errorf(
			"bug: no output named '%s' found in output schema (%w)",
			outputID, lastErrors,
		)
	}
	_, err = outputSchema.Unserialize(outputDataEntry.outputData)
	if err != nil {
		return "", nil, fmt.Errorf(
			"bug: output schema cannot unserialize output data (%s) (%w)",
			err.Error(), lastErrors,
		)
	}
	return outputDataEntry.outputID, outputData, nil
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
	callableFunctions map[string]schema.CallableFunction
	runningSteps      map[string]step.RunningStep
	outputDataChannel chan outputDataType
	outputDone        bool
	// waitingOutputs keeps track of all workflow output nodes to know when the workflow fails.
	waitingOutputs  map[string]dgraph.Node[*DAGItem]
	context         context.Context
	recentErrors    chan error
	cancel          context.CancelFunc
	workflowContext map[string][]byte
	lifecycles      map[string]step.Lifecycle[step.LifecycleStageWithSchema]
}

func (l *loopState) terminateAllSteps() {
	l.logger.Debugf("Terminating all steps...")
	for stepID, runningStep := range l.runningSteps {
		l.logger.Debugf("Terminating step %s...", stepID)
		if err := runningStep.ForceClose(); err != nil {
			panic(fmt.Errorf("failed to close step %s (%w)", stepID, err))
		}
	}
}

// getLastError gathers the last errors. If there are several, it creates a new one that consolidates
// all non-duplicate ones.
// This will read from the channel. Calling again will only gather new errors since the last call.
func (l *loopState) getLastError() error {
	errors := map[string]error{}
errGatherLoop:
	for {
		select {
		case err := <-l.recentErrors:
			errors[err.Error()] = err
		default:
			break errGatherLoop // No more errors
		}
	}
	switch len(errors) {
	case 0:
		fallthrough
	case 1:
		for _, err := range errors {
			return err
		}
		return nil
	default:
		errorsAsString := ""
		for errStr := range errors {
			errorsAsString += " " + errStr
		}
		return fmt.Errorf("multiple errors:%s", errorsAsString)
	}
}

func (l *loopState) handleErrors() error {
	lastErr := l.getLastError()
	if lastErr != nil {
		l.logger.Warningf("Workflow execution aborted with error: %s", lastErr.Error())
		return lastErr
	}
	return nil
}

func (l *loopState) onStageComplete(
	stepID string,
	previousStage *string,
	previousStageOutputID *string,
	previousStageOutput *any,
	wg *sync.WaitGroup,
) {
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
	l.logger.Debugf("Resolving node %q in the DAG on stage complete", stageNode.ID())
	if err := stageNode.ResolveNode(dgraph.Resolved); err != nil {
		errMessage := fmt.Errorf("failed to resolve stage node ID %s (%s)", stageNode.ID(), err.Error())
		l.recentErrors <- errMessage
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
		// Resolves the node in the DAG. This allows us to know which nodes are
		// ready for processing due to all dependencies being resolved.
		l.logger.Debugf("Resolving output node %q in the DAG", outputNode.ID())
		if err := outputNode.ResolveNode(dgraph.Resolved); err != nil {
			l.logger.Errorf("Failed to resolve output node ID %s (%w)", outputNode.ID(), err)
			l.recentErrors <- fmt.Errorf("failed to resolve output node ID %s (%w)", outputNode.ID(), err)
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
		// Mark all alternative output ID nodes as unresolvable.
		// Use the lifecycle to find the possible output IDs
		l.markOutputsUnresolvable(stepID, *previousStage, previousStageOutputID)

		// Placing data from the output into the general data structure
		l.data[WorkflowStepsKey].(map[string]any)[stepID].(map[string]any)[*previousStage] = map[string]any{}
		l.data[WorkflowStepsKey].(map[string]any)[stepID].(map[string]any)[*previousStage].(map[string]any)[*previousStageOutputID] = *previousStageOutput
	}
	l.notifySteps()
}

// Marks the outputs of that stage unresolvable.
// Optionally skip an output that should not unresolve.
// Does not (un)resolve the non-output node for that stage.
// To prevent a deadlock, `notifySteps()` should be called at some point after this is called.
func (l *loopState) markOutputsUnresolvable(stepID string, stageID string, skippedOutput *string) {
	stages := l.lifecycles[stepID].Stages
	for _, stage := range stages {
		if stage.ID != stageID {
			continue
		}
		for stageOutputID := range stage.Outputs {
			if skippedOutput != nil && stageOutputID == *skippedOutput {
				continue
			}
			unresolvableOutputNode, err := l.dag.GetNodeByID(GetOutputNodeID(stepID, stage.ID, stageOutputID))
			if err != nil {
				l.logger.Warningf("Could not get DAG node %s (%s)", stepID+"."+stage.ID+"."+stageOutputID, err.Error())
				continue
			}
			l.logger.Debugf("Will mark node %s in the DAG as unresolvable", stepID+"."+stage.ID+"."+stageOutputID)
			err = unresolvableOutputNode.ResolveNode(dgraph.Unresolvable)
			if err != nil {
				panic(fmt.Errorf("error while marking node %s in DAG as unresolvable (%s)", unresolvableOutputNode.ID(), err.Error()))
			}
		}
	}
}

// Marks the stage node for the step's stage as unresolvable.
// Does not (un)resolve the outputs of that node. For that, call markOutputsUnresolvable() instead or
// in addition to calling this function.
// To prevent a deadlock, `notifySteps()` should be called at some point after this is called.
func (l *loopState) markStageNodeUnresolvable(stepID string, stageID string) {
	unresolvableOutputNode, err := l.dag.GetNodeByID(GetStageNodeID(stepID, stageID))
	if err != nil {
		l.logger.Warningf("Could not get DAG node %s (%s)", stepID+"."+stageID, err.Error())
		return
	}
	l.logger.Debugf("Will mark node %s in the DAG as unresolvable", stepID+"."+stageID)
	err = unresolvableOutputNode.ResolveNode(dgraph.Unresolvable)
	if err != nil {
		panic(
			fmt.Errorf(
				"error while marking node %s in DAG as unresolvable (%s)",
				unresolvableOutputNode.ID(),
				err.Error(),
			),
		)
	}
}

// notifySteps is a function we can call to go through all DAG nodes that are marked
// ready and provides step inputs based on expressions.
// The lock should be acquired by the caller before this is called.
func (l *loopState) notifySteps() { //nolint:gocognit
	readyNodes := l.dag.PopReadyNodes()
	l.logger.Debugf("Currently %d DAG nodes are ready. Now processing them.", len(readyNodes))

	// Can include runnable nodes, nodes that cannot be resolved, and nodes that are not for running, like inputs.
	for nodeID, resolutionStatus := range readyNodes {
		failed := resolutionStatus == dgraph.Unresolvable
		l.logger.Debugf("Processing step node %s with resolution status %q", nodeID, resolutionStatus)
		node, err := l.dag.GetNodeByID(nodeID)
		if err != nil {
			panic(fmt.Errorf("failed to get node %s (%w)", nodeID, err))
		}
		nodeItem := node.Item()
		if failed {
			if nodeItem.Kind == DAGItemKindOutput {
				l.logger.Debugf("Output node %s failed", nodeID)
				// Check to see if there are any remaining output nodes, and if there aren't,
				// cancel the context.
				delete(l.waitingOutputs, nodeID)
				if len(l.waitingOutputs) == 0 && !l.outputDone {
					l.recentErrors <- &ErrNoMorePossibleOutputs{
						l.dag,
					}
					l.cancel()
				}
			} else {
				l.logger.Debugf("Disregarding failed node %s with type %s", nodeID, nodeItem.Kind)
			}
			continue
		}
		// The data structure that the particular node requires. One or more fields. May or may not contain expressions.
		inputData := nodeItem.Data
		if inputData == nil {
			switch nodeItem.Kind {
			case DagItemKindDependencyGroup:
				if err := node.ResolveNode(dgraph.Resolved); err != nil {
					panic(fmt.Errorf("error occurred while resolving workflow OR group node (%s)", err.Error()))
				}
				l.notifySteps() // Needs to be called after resolving a node.
				continue
			default:
				// No input data is needed. This is often the case for input nodes.
				continue
			}
		}

		// Resolve any expressions in the input data.
		// untypedInputData stores the resolved data
		untypedInputData, err := l.resolveExpressions(inputData, l.data)
		if err != nil {
			// An error here often indicates a locking issue in a step provider. This could be caused
			// by the lock not being held when the output was marked resolved.
			panic(fmt.Errorf("cannot resolve expressions for %s (%w)", nodeID, err))
		}

		// This switch checks to see if it's a node that needs to be run.
		switch nodeItem.Kind {
		case DAGItemKindStepStage:
			if nodeItem.DataSchema == nil {
				// This should only happen if the stage doesn't have any input fields.
				// This may not even get called. That should be checked.
				break
			}
			// We have a stage we can proceed with. Let's provide it with input.
			// Tries to match the schema
			if _, err := nodeItem.DataSchema.Unserialize(untypedInputData); err != nil {
				l.logger.Errorf("Bug: schema evaluation resulted in invalid data for %s (%v)", nodeID, err)
				l.recentErrors <- fmt.Errorf("bug: schema evaluation resulted in invalid data for %s (%w)", nodeID, err)
				l.cancel()
				return
			}

			// This check is here just to make sure it has the required fields set
			if nodeItem.StepID == "" || nodeItem.StageID == "" {
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
			if err := l.runningSteps[nodeItem.StepID].ProvideStageInput(
				nodeItem.StageID,
				typedInputData,
			); err != nil {
				l.logger.Errorf("Bug: failed to provide input to step %s (%w)", nodeItem.StepID, err)
				l.recentErrors <- fmt.Errorf("bug: failed to provide input to step %s (%w)", nodeItem.StepID, err)
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
					outputID:   nodeItem.OutputID,
					outputData: untypedInputData,
				}
				// Since this is the only thread accessing the channel, it should be
				// safe to close it now
				close(l.outputDataChannel)
			}

			if err := node.ResolveNode(dgraph.Resolved); err != nil {
				l.logger.Errorf("BUG: Error occurred while resolving workflow output node (%s)", err.Error())
			}
		default:
			panic(fmt.Errorf("unhandled case for type %s", nodeItem.Kind))

		}
	}
}

type stateCounters struct {
	starting int
	waiting  int
	running  int
	finished int
}

func (l *loopState) countStates() (counters stateCounters) {
	for stepID, runningStep := range l.runningSteps {
		switch runningStep.State() {
		case step.RunningStepStateStarting:
			counters.starting++
			l.logger.Debugf("Step %s is currently starting.", stepID)
		case step.RunningStepStateWaitingForInput:
			counters.waiting++
			l.logger.Debugf("Step %s is currently waiting.", stepID)
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
	hasReadyNodes := l.dag.HasReadyNodes()
	l.logger.Infof(
		"There are currently %d steps starting, %d waiting, %d running, %d finished. HasReadyNodes: %t",
		counters.starting,
		counters.waiting,
		counters.running,
		counters.finished,
		hasReadyNodes,
	)
	if counters.starting == 0 && counters.running == 0 && !hasReadyNodes && !l.outputDone {
		if retries <= 0 {
			l.recentErrors <- &ErrNoMorePossibleSteps{
				l.dag,
			}
			l.logger.Debugf("DAG:\n%s", l.dag.Mermaid())
			l.logger.Errorf("TERMINATING WORKFLOW; Errors below this error may be due to the early termination")
			l.cancel()
		} else {
			// Retry. There are times when all the steps are in a transition state.
			// Retrying will delay the check until after they are done with the transition.
			l.logger.Warningf("No running steps. Rechecking...")
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-time.After(time.Duration(5) * time.Millisecond):
					time.Sleep(5 * time.Millisecond)
					l.lock.Lock()
					l.checkForDeadlocks(retries-1, wg)
					l.lock.Unlock()
				case <-l.context.Done():
					return
				}
			}()
		}
	}
}

// resolveExpressions takes an inputData value potentially containing expressions and a dataModel containing data
// for expressions and resolves the expressions contained in inputData using reflection.
func (l *loopState) resolveExpressions(inputData any, dataModel any) (any, error) {
	switch expr := inputData.(type) {
	case expressions.Expression:
		l.logger.Debugf("Evaluating expression %s...", expr.String())
		return expr.Evaluate(dataModel, l.callableFunctions, l.workflowContext)
	case *infer.OneOfExpression:
		return l.resolveOneOfExpression(expr, dataModel)
	case *infer.OptionalExpression:
		return l.resolveOptionalExpression(expr, dataModel)
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
			if newValue != nil { // In case it's an optional field.
				result[key] = newValue
			}
		}
		return result, nil
	default:
		return inputData, nil
	}
}

func (l *loopState) resolveOneOfExpression(expr *infer.OneOfExpression, dataModel any) (any, error) {
	l.logger.Debugf("Evaluating oneof expression %s...", expr.String())

	// Get the node the OneOf uses to check which Or dependency resolved first (the others will either not be
	// in the resolved list, or they will be obviated)
	if expr.NodePath == "" {
		return nil, fmt.Errorf("node path is empty in oneof expression %s", expr.String())
	}
	oneOfNode, err := l.dag.GetNodeByID(expr.NodePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get node to resolve oneof expression (%w)", err)
	}
	dependencies := oneOfNode.ResolvedDependencies()
	firstResolvedDependency := ""
	for dependency, dependencyType := range dependencies {
		if dependencyType == dgraph.OrDependency {
			firstResolvedDependency = dependency
			break
		} else if dependencyType == dgraph.ObviatedDependency {
			l.logger.Infof("Multiple OR cases triggered; skipping %q", dependency)
		}
	}
	if firstResolvedDependency == "" {
		return nil, fmt.Errorf("could not find resolved dependency for oneof expression %q", expr.String())
	}
	optionID := strings.Replace(firstResolvedDependency, expr.NodePath+".", "", 1)
	optionExpr, found := expr.Options[optionID]
	if !found {
		return nil, fmt.Errorf("could not find oneof option %q for oneof %q", optionID, expr)
	}
	// Still pass the current node in due to the possibility of a foreach within a foreach.
	subTypeResolution, err := l.resolveExpressions(optionExpr, dataModel)
	if err != nil {
		return nil, err
	}

	// Validate that it returned a map type (this is required because oneof subtypes need to be objects)
	// With a special case for values from the providers, which are map[string]any instead of map[any]any
	// The output must be copied since it could be referenced several times.
	var outputData map[any]any
	switch subTypeObjectMap := subTypeResolution.(type) {
	case map[string]any:
		outputData = make(map[any]any, len(subTypeObjectMap))
		for k, v := range subTypeObjectMap {
			outputData[k] = v
		}
	case map[any]any:
		outputData = make(map[any]any, len(subTypeObjectMap))
		for k, v := range subTypeObjectMap {
			outputData[k] = v
		}
	default:
		return nil, fmt.Errorf("sub-type for oneof is not the serialized version of an object (a map); got %T", subTypeResolution)
	}
	// Now add the discriminator
	outputData[expr.Discriminator] = optionID

	return outputData, nil
}

func (l *loopState) resolveOptionalExpression(expr *infer.OptionalExpression, dataModel any) (any, error) {
	l.logger.Debugf("Evaluating oneof expression %s...", expr.Expr.String())
	if expr.ParentNodePath == "" {
		return nil, fmt.Errorf("ParentNodePath is empty in resolve optional expression %s", expr.Expr.String())
	}
	if expr.GroupNodePath == "" {
		return nil, fmt.Errorf("GroupNodePath is empty in resolve optional expression %s", expr.Expr.String())
	}
	// Check to see if the group node is resolved within the parent node
	parentDagNode, err := l.dag.GetNodeByID(expr.ParentNodePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent node to resolve optional expression (%w)", err)
	}
	resolvedDependencies := parentDagNode.ResolvedDependencies()
	_, dependencyGroupResolved := resolvedDependencies[expr.GroupNodePath]
	if !dependencyGroupResolved {
		return nil, nil // It's nil to indicate that the optional field is not present.
	}
	return expr.Expr.Evaluate(dataModel, l.callableFunctions, l.workflowContext)
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
	onStepStageFailure func(
		step step.RunningStep,
		stage string,
		wg *sync.WaitGroup,
		err error,
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

func (s stageChangeHandler) OnStepStageFailure(
	step step.RunningStep,
	stage string,
	wg *sync.WaitGroup,
	err error,
) {
	s.onStepStageFailure(step, stage, wg, err)
}

// PrintObjectNamespaceTable constructs and writes a tidy table of workflow
// Objects and their namespaces to the given output destination.
func PrintObjectNamespaceTable(output io.Writer, allNamespaces map[string]map[string]*schema.ObjectSchema, logger log.Logger) {
	if len(allNamespaces) == 0 {
		logger.Warningf("No namespaces found in workflow")
		return
	}
	groupLists := tablefmt.ExtractGroupedLists[*schema.ObjectSchema](allNamespaces)
	df := tablefmt.UnnestLongerSorted(groupLists)
	df = tablefmt.SwapColumns(df)
	tableprinter.PrintTwoColumnTable(output, []string{"object", "namespace"}, df)
}

package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.arcalot.io/dgraph"
	"go.arcalot.io/log"
	"go.flow.arcalot.io/engine/config"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/engine/internal/deploy/registry"
	"go.flow.arcalot.io/engine/internal/expand"
	"go.flow.arcalot.io/engine/internal/yaml"
	"go.flow.arcalot.io/engine/workflow"
	"go.flow.arcalot.io/pluginsdk/atp"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// WorkflowEngine is responsible for executing workflows and returning their result.
type WorkflowEngine interface {
	// RunWorkflow executes a workflow from the passed workflow files as parameters. One of the files must be designated
	// as a workflow file, which will be parsed from the YAML format. Additional files may be passed so that the
	// workflow may access them (e.g. a kubeconfig file). The workflow input is passed as a separate file.
	RunWorkflow(
		ctx context.Context,
		input []byte,
		files map[string][]byte,
		workflowFileName string,
	) (
		outputData any,
		err error,
	)
}

type workflowEngine struct {
	logger           log.Logger
	deployerRegistry registry.Registry
	config           *config.Config
}

//nolint:funlen,gocognit,gocyclo
func (w workflowEngine) RunWorkflow(
	ctx context.Context,
	input []byte,
	files map[string][]byte,
	workflowFileName string,
) (outputData any, err error) {
	wf, data, err := w.loadWorkflow(workflowFileName, files)
	if err != nil {
		return nil, err
	}

	inputSchema := wf.Input
	parsedInput, err := yaml.New().Parse(input)
	if err != nil {
		return nil, ErrInvalidInputYAML{
			err,
		}
	}
	unserializedInput, err := inputSchema.Unserialize(parsedInput.Raw())
	if err != nil {
		return nil, ErrInvalidInput{
			err,
		}
	}

	// We lost YAML tags during unserialization, we need to reconstruct them. The output, as well as step input and
	// deploy will contain YAML nodes after this.
	output, ok := data.MapKey("output")
	if !ok {
		// Map key does not exist.
		wf.Output = yaml.EmptyNode()
	} else {
		wf.Output = output
	}

	steps, ok := data.MapKey("steps")
	if !ok {
		// This should never happen
		return nil, ErrNoSteps
	}

	for _, stepID := range steps.MapKeys() {
		step, _ := steps.MapKey(stepID)
		if deploy, ok := step.MapKey("deploy"); ok {
			wf.Steps[stepID].Deploy = deploy
		}
		if input, ok := step.MapKey("input"); ok {
			wf.Steps[stepID].Input = input
		}
	}

	stepSchemas, err := w.getStepSchemas(wf)
	if err != nil {
		return nil, err
	}

	w.logger.Infof("Building dependency tree...")
	// Now we have the YAML tags replaced, let's construct a dependency tree.
	var depTree dgraph.DirectedGraph[treeItem]
	depTree, wf.Steps, wf.Output, err = buildDependencyTree(
		inputSchema,
		wf.Steps,
		stepSchemas,
		wf.Output.(yaml.Node),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependency tree (%w)", err)
	}
	if depTree.HasCycles() {
		return nil, fmt.Errorf("step dependency tree has at least one cycle")
	}
	outputNode, err := depTree.GetNodeByID("output")
	if err != nil {
		return nil, fmt.Errorf("failed to find output key")
	}
	w.logger.Infof("Dependency tree complete.")
	w.logger.Infof("Dependency tree Mermaid:\n%s", depTree.Mermaid())

	lock := &sync.Mutex{}
	// dataStructure holds the data structure that can be queried using the expression language.
	dataStructure := map[string]any{
		"input": unserializedInput,
		"steps": map[string]any{},
	}

	finishedSteps := make(chan dgraph.Node[treeItem], len(wf.Steps))
	finishedOutputs := make(chan dgraph.Node[treeItem], len(wf.Steps))
	runningSteps := map[string]struct{}{}

	for _, node := range depTree.ListNodes() {
		if node.Item().Type == inputKey {
			if err := node.Remove(); err != nil {
				return nil, fmt.Errorf("failed to remove input node from DAG (%w)", err)
			}
		}
	}

	plugins := map[string]deployer.Plugin{}
	defer func() {
		lock.Lock()
		for name, plugin := range plugins {
			if err := plugin.Close(); err != nil {
				w.logger.Errorf("Failed to undeploy plugin %s (%v)", name, err)
			}
		}
		lock.Unlock()
	}()

	waitForFinish := func() error {
		step := <-finishedSteps
		output := <-finishedOutputs
		lock.Lock()
		w.logger.Infof("Step %s has finished with output %s.", step.Item().Item, output.Item().Output)
		w.logger.Debugf("Removing dependency tree node %s...", step.ID())
		if err := step.Remove(); err != nil {
			return err
		}
		w.logger.Debugf("Removing dependency tree node %s...", output.ID())
		if err := output.Remove(); err != nil {
			return err
		}
		delete(runningSteps, step.ID())
		lock.Unlock()
		return nil
	}

mainloop:
	for {
		var nodesWithoutInboundConnections map[string]dgraph.Node[treeItem]
		for {
			nodesWithoutInboundConnections = filterStepNodes(depTree.ListNodesWithoutInboundConnections(), runningSteps)
			if len(nodesWithoutInboundConnections) == 0 {
				if len(runningSteps) == 0 {
					break mainloop
				}
				if err := waitForFinish(); err != nil {
					return nil, err
				}
			} else {
				break
			}
		}
		for id := range nodesWithoutInboundConnections {
			runningSteps[id] = struct{}{}
		}
		for _, node := range nodesWithoutInboundConnections {
			node := node
			item := node.Item()
			step := wf.Steps[item.Item]

			w.logger.Infof("Starting step %s...", item.Item)

			lock.Lock()
			expandedDeploy, err := expand.Datastructure(step.Deploy, dataStructure)
			if err != nil {
				lock.Unlock()
				return nil, fmt.Errorf(
					"failed to expand step %s deploy parameters (%w)",
					item.Item,
					err,
				)
			}
			expandedInput, err := expand.Datastructure(step.Input, dataStructure)
			if err != nil {
				lock.Unlock()
				return nil, fmt.Errorf(
					"failed to expand step %s input parameters (%w)",
					item.Item,
					err,
				)
			}
			lock.Unlock()

			if _, err := stepSchemas[item.Item].Input().Unserialize(expandedInput); err != nil {
				return nil, fmt.Errorf(
					"expanded input for step %s does not match input schema (%w)",
					item.Item,
					err,
				)
			}

			if len(expandedDeploy.(map[any]any)) == 0 {
				expandedDeploy = w.config.LocalDeployer
			}
			unserializedExpandedDeployerConfig, err := w.deployerRegistry.Schema().Unserialize(expandedDeploy)
			if err != nil {
				return nil, fmt.Errorf("failed to unserialize deployer config for step %s (%w)", item.Item, err)
			}

			deployerConnector, err := w.deployerRegistry.Create(unserializedExpandedDeployerConfig, w.logger)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to instantiate deployer for step %s (%w)",
					item.Item,
					err,
				)
			}
			container, err := deployerConnector.Deploy(context.Background(), step.Plugin)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to deploy plugin for step %s (%w)",
					item.Item,
					err,
				)
			}
			lock.Lock()
			plugins[item.Item] = container
			lock.Unlock()
			atpClient := atp.NewClientWithLogger(container, w.logger)
			pluginSchema, err := atpClient.ReadSchema()
			if err != nil {
				return nil, fmt.Errorf(
					"failed to read schema for step %s", item.Item,
				)
			}
			selectedStep := step.Step
			if selectedStep == "" {
				if len(pluginSchema.Steps()) != 1 {
					// This shouldn't happen, we checked before
					return nil, fmt.Errorf(
						"plugin %s declares more than one step and no step was provided", step.Plugin,
					)
				}
				for stepID := range pluginSchema.Steps() {
					selectedStep = stepID
				}
			}

			go func() {
				defer func() {
					finishedSteps <- node
				}()
				outputID, outputData, err := atpClient.Execute(ctx, selectedStep, expandedInput)
				if err != nil {
					panic(err)
				}
				lock.Lock()
				defer lock.Unlock()
				dataStructure["steps"].(map[string]any)[item.Item] = map[string]map[string]any{
					"outputs": {
						outputID: outputData,
					},
				}
				outputNode, err := depTree.GetNodeByID(fmt.Sprintf("%s.%s.%s.%s", stepsKey, item.Item, outputsKey, outputID))
				if err != nil {
					panic(err)
				}
				finishedOutputs <- outputNode
			}()
			w.logger.Infof("Step %s is now running...", item.Item)
		}
		if err := waitForFinish(); err != nil {
			return nil, err
		}
	}

	outputDeps, err := outputNode.ListInboundConnections()
	if err != nil {
		return nil, fmt.Errorf("listing output dependencies failed (%w)", err)
	}
	if len(outputDeps) > 0 {
		outputList := make([]string, len(outputDeps))
		i := 0
		for dep := range outputDeps {
			outputList[i] = dep
		}
		return nil, fmt.Errorf(
			"workflow run failed, output has unsuccessful dependencies: %s",
			strings.Join(outputList, "; "),
		)
	}

	d, err := expand.Datastructure(wf.Output, dataStructure)
	if err != nil {
		w.logger.Errorf("workflow run failed, cannot obtain output data (%w)", err)
	}
	return d, err
}

func filterStepNodes(connections map[string]dgraph.Node[treeItem], runningSteps map[string]struct{}) map[string]dgraph.Node[treeItem] {
	result := map[string]dgraph.Node[treeItem]{}
	for k, v := range connections {
		if v.Item().Type != stepsKey || v.Item().Output != "" {
			continue
		}
		if _, ok := runningSteps[k]; ok {
			continue
		}
		result[k] = v
	}
	return result
}

func (w workflowEngine) loadWorkflow(
	workflowFileName string,
	files map[string][]byte,
) (*workflow.Workflow, yaml.Node, error) {
	if workflowFileName == "" {
		workflowFileName = "workflow.yaml"
	}
	workflowContents, ok := files[workflowFileName]
	if !ok {
		return nil, nil, ErrNoWorkflowFile
	}

	if len(workflowContents) == 0 {
		return nil, nil, ErrEmptyWorkflowFile
	}

	data, err := yaml.New().Parse(workflowContents)
	if err != nil {
		return nil, nil, ErrInvalidWorkflowYAML{err}
	}

	unserializedWorkflow, err := workflow.GetSchema().UnserializeType(data.Raw())
	if err != nil {
		return nil, nil, ErrInvalidWorkflow{err}
	}

	return unserializedWorkflow, data, nil
}

func (w workflowEngine) getStepSchemas(wf *workflow.Workflow) (map[string]schema.Step, error) {
	unserializedLocalDeployerConfig, err := w.deployerRegistry.Schema().Unserialize(w.config.LocalDeployer)
	if err != nil {
		return nil, fmt.Errorf("failed to unserialize local deployer config (%w)", err)
	}
	localDeployer, err := w.deployerRegistry.Create(unserializedLocalDeployerConfig, w.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid local deployer configuration (%w)", err)
	}
	w.logger.Infof("Loading plugins locally to determine schemas...")
	stepSchemas := make(map[string]schema.Step, len(wf.Steps))
	for stepID, step := range wf.Steps {
		ctx, cancel := context.WithCancel(context.Background())
		w.logger.Infof("Deploying %s...", step.Plugin)
		p, err := localDeployer.Deploy(ctx, step.Plugin)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to deploy plugin from image %s (%w)", step.Plugin, err)
		}
		transport := atp.NewClientWithLogger(p, w.logger)
		s, err := transport.ReadSchema()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to read plugin schema from %s (%w)", step.Plugin, err)
		}
		if err := p.Close(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to shut down local plugin from %s (%w)", step.Plugin, err)
		}
		cancel()
		selectedStep := ""
		if len(s.Steps()) == 0 {
			return nil, fmt.Errorf(
				"plugin %s does not declare any steps", step.Plugin)
		}
		if step.Step == "" {
			if len(s.Steps()) > 1 {
				return nil, fmt.Errorf(
					"step value required for plugin %s because it declares more than one step", step.Plugin)
			}
			for selectedStepID := range s.Steps() {
				selectedStep = selectedStepID
			}
		} else {
			selectedStep = step.Step
		}
		if _, ok := s.Steps()[selectedStep]; !ok {
			return nil, fmt.Errorf(
				"plugin %s does not have a step named %s", step.Plugin, selectedStep)
		}

		stepSchemas[stepID] = s.Steps()[selectedStep]
		w.logger.Infof("Schema for %s obtained.", step.Plugin)
	}
	w.logger.Infof("Schema loading complete.")
	return stepSchemas, nil
}

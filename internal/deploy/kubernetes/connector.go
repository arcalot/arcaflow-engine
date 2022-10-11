package kubernetes

import (
	"context"
	"fmt"
	"io"

	kubernetesDeploy "go.flow.arcalot.io/engine/deploy/kubernetes"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	core "k8s.io/api/core/v1"
	kubeErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/remotecommand"
	watchTools "k8s.io/client-go/tools/watch"
)

type connector struct {
	cli              *kubernetes.Clientset
	restClient       *restclient.RESTClient
	config           *kubernetesDeploy.Config
	connectionConfig restclient.Config
}

//nolint:funlen
func (c connector) Deploy(ctx context.Context, image string) (deployer.Plugin, error) {
	podSpec := c.config.Deployment.Spec.PodSpec

	pluginContainer := c.config.Deployment.Spec.PluginContainer
	pluginContainer.Stdin = true
	pluginContainer.Image = image
	pluginContainer.Env = append(pluginContainer.Env, core.EnvVar{
		Name:  "PYTHON_UNBUFFERED",
		Value: "1",
	})
	pluginContainer.Args = []string{"--atp"}

	podSpec.Containers = append(
		podSpec.Containers,
		pluginContainer,
	)

	meta := c.config.Deployment.Metadata
	if meta.Name == "" && meta.GenerateName == "" {
		meta.GenerateName = "arcaflow-plugin-"
	}

	pod, err := c.cli.CoreV1().Pods(c.config.Deployment.Metadata.Namespace).Create(
		ctx,
		&core.Pod{
			ObjectMeta: meta,
			Spec:       podSpec,
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod (%w)", err)
	}

	pod, err = c.waitForPod(ctx, pod)
	if err != nil {
		_ = c.removePod(ctx, pod, true)
		return nil, err
	}

	req := c.restClient.Post().
		Namespace(c.config.Deployment.Metadata.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("attach")
	req.VersionedParams(
		&core.PodAttachOptions{
			Container: pod.Spec.Containers[len(podSpec.Containers)-1].Name,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec,
	)

	podExec, err := remotecommand.NewSPDYExecutor(
		&c.connectionConfig,
		"POST",
		req.URL(),
	)
	if err != nil {
		_ = c.removePod(ctx, pod, true)
		return nil, err
	}

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	go func() {
		defer func() {
			_ = stdoutWriter.Close()
			_ = stdinWriter.Close()
		}()
		_ = podExec.Stream(
			remotecommand.StreamOptions{
				Stdin:  stdinReader,
				Stdout: stdoutWriter,
				Stderr: stdoutWriter,
			},
		)
	}()

	return &connectorContainer{
		pod:          pod,
		connector:    c,
		stdinWriter:  stdinWriter,
		stdoutReader: stdoutReader,
	}, nil
}

func (c connector) waitForPod(ctx context.Context, pod *core.Pod) (*core.Pod, error) {
	fieldSelector := fields.
		OneTermEqualSelector("metadata.name", pod.Name).
		String()
	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return c.cli.
				CoreV1().
				Pods(c.config.Deployment.Metadata.Namespace).
				List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return c.cli.
				CoreV1().
				Pods(c.config.Deployment.Metadata.Namespace).
				Watch(ctx, options)
		},
	}
	event, err := watchTools.UntilWithSync(
		ctx,
		listWatch,
		&core.Pod{},
		nil,
		c.isPodAvailableEvent,
	)
	if event != nil {
		pod = event.Object.(*core.Pod)
	}
	return pod, err
}

func (c connector) isPodAvailableEvent(event watch.Event) (bool, error) {
	if event.Type == watch.Deleted {
		return false, kubeErrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}

	if eventObject, ok := event.Object.(*core.Pod); ok {
		switch eventObject.Status.Phase {
		case core.PodFailed, core.PodSucceeded:
			return true, nil
		case core.PodRunning:
			conditions := eventObject.Status.Conditions
			for _, condition := range conditions {
				if condition.Type == core.PodReady &&
					condition.Status == core.ConditionTrue {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (c connector) removePod(ctx context.Context, pod *core.Pod, force bool) error {
	var gracePeriod *int64
	if force {
		t := int64(0)
		gracePeriod = &t
	}
	return c.cli.CoreV1().Pods(c.config.Deployment.Metadata.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: gracePeriod,
	})
}

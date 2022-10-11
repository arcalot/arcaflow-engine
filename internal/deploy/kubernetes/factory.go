package kubernetes

import (
	"fmt"

	kubernetesDeploy "go.flow.arcalot.io/engine/deploy/kubernetes"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
)

// NewFactory creates a new factory for the Docker deployer.
func NewFactory() deployer.ConnectorFactory[*kubernetesDeploy.Config] {
	return &factory{}
}

type factory struct {
}

func (f factory) ID() string {
	return "kubernetes"
}

func (f factory) ConfigurationSchema() *schema.TypedScopeSchema[*kubernetesDeploy.Config] {
	return kubernetesDeploy.Schema
}

func (f factory) Create(config *kubernetesDeploy.Config) (deployer.Connector, error) {
	connectionConfig := f.createConnectionConfig(config)

	cli, err := kubernetes.NewForConfig(&connectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes config (%w)", err)
	}

	restClient, err := restclient.RESTClientFor(&connectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes REST client (%w)", err)
	}

	return &connector{
		cli:              cli,
		restClient:       restClient,
		config:           config,
		connectionConfig: connectionConfig,
	}, nil
}

func (f factory) createConnectionConfig(config *kubernetesDeploy.Config) restclient.Config {
	return restclient.Config{
		Host:    config.Connection.Host,
		APIPath: config.Connection.APIPath,
		ContentConfig: restclient.ContentConfig{
			GroupVersion:         &core.SchemeGroupVersion,
			NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		},
		Username:    config.Connection.Username,
		Password:    config.Connection.Password,
		BearerToken: config.Connection.BearerToken,
		Impersonate: restclient.ImpersonationConfig{},
		TLSClientConfig: restclient.TLSClientConfig{
			ServerName: config.Connection.ServerName,
			CertData:   []byte(config.Connection.CertData),
			KeyData:    []byte(config.Connection.KeyData),
			CAData:     []byte(config.Connection.CAData),
		},
		UserAgent: "Arcaflow",
		QPS:       float32(config.Connection.QPS),
		Burst:     int(config.Connection.Burst),
		Timeout:   config.Timeouts.HTTP,
	}
}

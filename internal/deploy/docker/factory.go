package docker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

	"github.com/docker/docker/client"
	"go.arcalot.io/log"
	"go.flow.arcalot.io/engine/deploy/docker"
	"go.flow.arcalot.io/engine/internal/deploy/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// NewFactory creates a new factory for the Docker deployer.
func NewFactory() deployer.ConnectorFactory[*docker.Config] {
	return &factory{}
}

type factory struct {
}

func (f factory) ID() string {
	return "docker"
}

func (f factory) ConfigurationSchema() *schema.TypedScopeSchema[*docker.Config] {
	return docker.Schema
}

func (f factory) Create(config *docker.Config, logger log.Logger) (deployer.Connector, error) {
	httpClient, err := f.getHTTPClient(config)
	if err != nil {
		return nil, err
	}

	cli, err := client.NewClientWithOpts(
		client.WithAPIVersionNegotiation(),
		client.WithHost(config.Connection.Host),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker container (%w)", err)
	}

	return &connector{
		cli,
		config,
		logger,
	}, nil
}

func (f factory) getHTTPClient(config *docker.Config) (*http.Client, error) {
	var httpClient *http.Client
	if config.Connection.CACert != "" && config.Connection.Key != "" && config.Connection.Cert != "" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS13,
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(config.Connection.CACert))
		tlsConfig.RootCAs = caCertPool

		keyPair, err := tls.X509KeyPair([]byte(config.Connection.Cert), []byte(config.Connection.Key))
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{keyPair}
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   config.Timeouts.HTTP,
		}
	} else if strings.HasPrefix(config.Connection.Host, "http://") {
		httpClient = &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   config.Timeouts.HTTP,
		}
	}
	return httpClient, nil
}

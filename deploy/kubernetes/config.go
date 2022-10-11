package kubernetes

import (
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Config holds the configuration for deployment via the Kubernetes API.
type Config struct {
	Connection Connection `json:"connection,omitempty" yaml:"connection,omitempty"`
	Pod        Pod        `json:"pod,omitempty" yaml:"deployment,omitempty"`
	Timeouts   Timeouts   `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
}

// Validate checks for conformity with the schema.
func (c *Config) Validate() error {
	return Schema.Validate(c)
}

// Connection describes how to connect to the Kubernetes API.
type Connection struct {
	Host    string `json:"host,omitempty" yaml:"host,omitempty"`
	APIPath string `json:"path,omitempty" yaml:"path,omitempty"`

	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`

	ServerName string `json:"serverName,omitempty" yaml:"serverName,omitempty"`

	CertData string `json:"cert,omitempty" yaml:"cert,omitempty"`
	KeyData  string `json:"key,omitempty" yaml:"key,omitempty"`
	CAData   string `json:"cacert,omitempty" yaml:"cacert,omitempty"`

	BearerToken string `json:"bearerToken,omitempty" yaml:"bearerToken,omitempty"`

	QPS   float64 `json:"qps,omitempty" yaml:"qps,omitempty"`
	Burst int64   `json:"burst,omitempty" yaml:"burst,omitempty"`
}

// Pod describes the pod to launch.
type Pod struct {
	Metadata metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Spec     PodSpec           `json:"spec,omitempty" yaml:"spec,omitempty"`
}

// Timeouts configures the various timeouts for the Kubernetes backend.
type Timeouts struct {
	HTTP time.Duration `json:"http,omitempty" yaml:"http"`
}

// PodSpec contains the specification of the pod to launch.
type PodSpec struct {
	v1.PodSpec `json:",inline"`

	// PluginContainer describes the container the plugin should be run in.
	PluginContainer v1.Container `json:"pluginContainer"`
}

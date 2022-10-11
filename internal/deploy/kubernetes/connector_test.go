package kubernetes_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"go.arcalot.io/assert"
	kubernetesDeploy "go.flow.arcalot.io/engine/deploy/kubernetes"
	"go.flow.arcalot.io/engine/internal/deploy/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

func TestSimpleInOut(t *testing.T) {
	dirname, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("Skipping test, cannot find user home directory (%v)", err)
	}
	cfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: dirname + "/.kube/config"},
		&clientcmd.ConfigOverrides{ClusterInfo: api.Cluster{Server: ""}})
	kubeconfig, err := cfg.ClientConfig()
	if err != nil {
		t.Skipf("Skipping test, load kubeconfig file from user home directory (%v)", err)
	}
	namespace, _, err := cfg.Namespace()
	if err != nil {
		t.Skipf("Skipping test, load kubeconfig file from user home directory (%v)", err)
	}

	configStruct := kubernetesDeploy.Config{
		Connection: kubernetesDeploy.Connection{
			Host:        kubeconfig.Host,
			APIPath:     kubeconfig.APIPath,
			Username:    kubeconfig.Username,
			Password:    kubeconfig.Password,
			ServerName:  kubeconfig.ServerName,
			CertData:    string(kubeconfig.CertData),
			KeyData:     string(kubeconfig.KeyData),
			CAData:      string(kubeconfig.CAData),
			BearerToken: kubeconfig.BearerToken,
			QPS:         float64(kubeconfig.QPS),
			Burst:       int64(kubeconfig.Burst),
		},
		Deployment: kubernetesDeploy.Deployment{
			Metadata: metav1.ObjectMeta{
				Namespace: namespace,
			},
		},
	}

	factory := kubernetes.NewFactory()
	schema := factory.ConfigurationSchema()
	serializedConfig, err := schema.SerializeType(&configStruct)
	assert.NoError(t, err)
	unserializedConfig, err := schema.UnserializeType(serializedConfig)
	assert.NoError(t, err)
	connector, err := factory.Create(unserializedConfig)
	assert.NoError(t, err)

	container, err := connector.Deploy(context.Background(), "quay.io/joconnel/io-test-script")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, container.Close())
	})

	var containerInput = []byte("abc\n")
	assert.NoErrorR[int](t)(container.Write(containerInput))

	buf := new(strings.Builder)
	assert.NoErrorR[int64](t)(io.Copy(buf, container))
	assert.Contains(t, buf.String(), "This is what input was received: \"abc\"")
}

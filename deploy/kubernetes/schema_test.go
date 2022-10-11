package kubernetes //nolint:testpackage
import (
	"testing"

	"go.arcalot.io/assert"
)

func TestIdentifier(t *testing.T) {
	for _, item := range []any{"i", "1", "test"} {
		t.Run(item.(string), func(t *testing.T) {
			unserializedData, err := identifier.Unserialize(item)
			assert.NoError(t, err)
			assert.Equals(t, unserializedData.(string), item.(string))
			assert.NoError(t, identifier.Validate(item))
		})
	}
	for _, item := range []any{"", "Hello world!", "$"} {
		t.Run(item.(string), func(t *testing.T) {
			_, err := identifier.Unserialize(item)
			if err == nil {
				t.Fatalf("No error returned")
			}
		})
	}
}

func TestImageTag(t *testing.T) {
	for _, item := range []any{"quay.io/arcalot/example:latest", "quay.io/arcalot/example", "arcalot/example"} {
		t.Run(item.(string), func(t *testing.T) {
			unserializedData, err := imageTag.Unserialize(item)
			assert.NoError(t, err)
			assert.Equals(t, unserializedData.(string), item.(string))
			assert.NoError(t, imageTag.Validate(item))
		})
	}
	for _, item := range []any{"", "Hello world!", "$"} {
		t.Run(item.(string), func(t *testing.T) {
			_, err := imageTag.Unserialize(item)
			if err == nil {
				t.Fatalf("No error returned")
			}
		})
	}
}

func TestLabelName(t *testing.T) {
	for _, item := range []any{"kubernetes.io/test", "test"} {
		t.Run(item.(string), func(t *testing.T) {
			unserializedData, err := labelName.Unserialize(item)
			assert.NoError(t, err)
			assert.Equals(t, unserializedData.(string), item.(string))
			assert.NoError(t, labelName.Validate(item))
		})
	}
	for _, item := range []any{"", "kubernetes.io/test/test", "$"} {
		t.Run(item.(string), func(t *testing.T) {
			_, err := labelName.Unserialize(item)
			if err == nil {
				t.Fatalf("No error returned")
			}
		})
	}
}

func TestLabelValue(t *testing.T) {
	for _, item := range []any{"", "kubernetes.io_test", "test"} {
		t.Run(item.(string), func(t *testing.T) {
			unserializedData, err := labelValue.Unserialize(item)
			assert.NoError(t, err)
			assert.Equals(t, unserializedData.(string), item.(string))
			assert.NoError(t, labelValue.Validate(item))
		})
	}
	for _, item := range []any{"kubernetes.io/test", "kubernetes.io/test/test", "$"} {
		t.Run(item.(string), func(t *testing.T) {
			_, err := labelValue.Unserialize(item)
			if err == nil {
				t.Fatalf("No error returned")
			}
		})
	}
}

func TestDNSSubdomainName(t *testing.T) {
	for _, item := range []any{"arcalot", "arcalot_io", "arcalot123"} {
		t.Run(item.(string), func(t *testing.T) {
			unserializedData, err := dnsSubdomainName.Unserialize(item)
			assert.NoError(t, err)
			assert.Equals(t, unserializedData.(string), item.(string))
			assert.NoError(t, dnsSubdomainName.Validate(item))
		})
	}
	for _, item := range []any{"arcalot.", "kubernetes.io/test/test", "$"} {
		t.Run(item.(string), func(t *testing.T) {
			_, err := dnsSubdomainName.Unserialize(item)
			if err == nil {
				t.Fatalf("No error returned")
			}
		})
	}
}

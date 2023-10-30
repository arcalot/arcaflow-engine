package config_test

import (
	"go.arcalot.io/log/v2"
	"testing"

	"go.arcalot.io/lang"
	"go.flow.arcalot.io/engine/config"
	"gopkg.in/yaml.v3"
)

var configLoadData = map[string]struct {
	input          string
	error          bool
	expectedOutput *config.Config
}{
	"empty": {
		input: "",
		expectedOutput: &config.Config{
			TypeHintPlugins: nil,
			LocalDeployers: map[string]any{
				"image": map[string]string{"deployer_id": "docker"},
			},
			Log: log.Config{
				Level:       log.LevelInfo,
				Destination: log.DestinationStdout,
			},
		},
	},
	"log-level": {
		input: `
log:
  level: debug
`,
		expectedOutput: &config.Config{
			TypeHintPlugins: nil,
			LocalDeployers: map[string]any{
				"image": map[string]string{"deployer_id": "docker"},
			},
			Log: log.Config{
				Level:       log.LevelDebug,
				Destination: log.DestinationStdout,
			},
		},
	},
	"type-kubernetes": {
		input: `
deployers:
  image: 
    deployer_id: kubernetes
`,
		expectedOutput: &config.Config{
			TypeHintPlugins: nil,
			LocalDeployers: map[string]any{
				"image": map[string]string{"deployer_id": "kubernetes"},
			},
			Log: log.Config{
				Level:       log.LevelInfo,
				Destination: log.DestinationStdout,
			},
		},
	},
	"plugins": {
		input: `
plugins:
  - quay.io/arcalot/example-plugin:latest
`,
		expectedOutput: &config.Config{
			TypeHintPlugins: []string{
				"quay.io/arcalot/example-plugin:latest",
			},
			LocalDeployers: map[string]any{
				"image": map[string]string{"deployer_id": "docker"},
			},
			Log: log.Config{
				Level:       log.LevelInfo,
				Destination: log.DestinationStdout,
			},
		},
	},
}

func TestConfigLoad(t *testing.T) {
	for name, tc := range configLoadData {
		testCase := tc
		t.Run(name, func(t *testing.T) {
			var data map[string]any
			if err := yaml.Unmarshal([]byte(testCase.input), &data); err != nil {
				t.Fatal(err)
			}
			c, err := config.Load(data)
			if err != nil && !tc.error {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err == nil && tc.error {
				t.Fatal("No error returned")
			}

			marshalledC := string(lang.Must2(yaml.Marshal(*c)))
			marshalledExpectedOutput := string(lang.Must2(yaml.Marshal(*testCase.expectedOutput)))

			if marshalledC != marshalledExpectedOutput {
				t.Fatalf(
					"The loaded config does not match the expected value:\n\nGot:\n\n%s\n\nExpected:\n\n%s\n\n",
					marshalledC,
					marshalledExpectedOutput,
				)
			}
		})
	}
}

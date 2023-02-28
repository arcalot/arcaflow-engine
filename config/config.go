package config

import log "go.arcalot.io/log/v2"

// StepOutputLogConfig is a config value for step output logging.
type StepOutputLogConfig struct {
	// The log level if output is encountered
	LogLevel log.Level `json:"level" yaml:"level"`
}

// Config is the main configuration structure that configures the engine for execution. It is not identical to the
// workflow being executed.
type Config struct {
	// TypeHintPlugins holds a list of plugins that will be used when building a type hint (e.g. JSONSchema) file for
	// workflows.
	TypeHintPlugins []string `json:"plugins" yaml:"plugins"`
	// LocalDeployer holds the configuration for executing plugins locally. This deployer is used to obtain the schema
	// from the plugins before executing them in a remote environment.
	LocalDeployer any `json:"deployer" yaml:"deployer"`
	// Log configures logging for workflow runs.
	Log log.Config `json:"log" yaml:"log"`
	// StepOutputLogging allows logging of step output
	LoggedOutputConfigs map[string]*StepOutputLogConfig `json:"logged_outputs" yaml:"logged_outputs"`
}

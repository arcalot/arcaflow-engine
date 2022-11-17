package config

import "fmt"

// Load loads a configuration data set into the config struct.
func Load(configData any) (*Config, error) {
	return getConfigSchema().UnserializeType(configData)
}

// Default returns the default configuration.
func Default() *Config {
	cfg, err := getConfigSchema().UnserializeType(map[string]any{})
	if err != nil {
		panic(fmt.Errorf("failed to obtain default configuration (%w)", err))
	}
	return cfg
}

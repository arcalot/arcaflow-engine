package config

// Load loads a configuration data set into the config struct.
func Load(configData any) (*Config, error) {
	return getConfigSchema().UnserializeType(configData)
}

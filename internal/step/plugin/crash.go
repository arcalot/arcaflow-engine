package plugin

// Crashed describes the error that happened when a plugin crashed.
type Crashed struct {
	Output string `json:"output"`
}

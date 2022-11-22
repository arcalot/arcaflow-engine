package plugin

// DeployFailed describes the error that happened during deployment.
type DeployFailed struct {
	Error string `json:"error"`
}

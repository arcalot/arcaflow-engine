package docker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"go.flow.arcalot.io/pluginsdk/schema"
)

func getDefaultDockerSocket() string {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		return "npipe:////./pipe/docker_engine"
	}
	return "unix:///var/run/docker.sock"
}

func mustJSONEncode(value any) string {
	v, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Errorf("failed to JSON encode value: %v", value))
	}
	return string(v)
}

func base64Decode(value string) string {
	v, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		panic(fmt.Errorf("failed to Base64 decode value: %s", value))
	}
	return string(v)
}

var configSchema = schema.NewTypedScopeSchema[*Config](
	schema.NewStructMappedObjectSchema[*Config](
		"Config",
		map[string]*schema.PropertySchema{
			"connection": schema.NewPropertySchema(
				schema.NewRefSchema("Connection", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Connection"),
					schema.PointerTo("Docker connection information."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"deployment": schema.NewPropertySchema(
				schema.NewRefSchema("Deployment", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Deployment"),
					schema.PointerTo("Deployment configuration for the plugin."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"timeouts": schema.NewPropertySchema(
				schema.NewRefSchema("Timeouts", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Timeouts"),
					schema.PointerTo("Timeouts for the Docker connection."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
		},
	),
	schema.NewStructMappedObjectSchema[Timeouts](
		"Timeouts",
		map[string]*schema.PropertySchema{
			"http": schema.NewPropertySchema(
				schema.NewIntSchema(schema.PointerTo(int64(100*time.Millisecond)), nil, &schema.UnitDurationNanoseconds),
				schema.NewDisplayValue(
					schema.PointerTo("HTTP"),
					schema.PointerTo("HTTP timeout for the Docker API."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(mustJSONEncode("15s")),
				nil,
			),
		},
	),
	schema.NewStructMappedObjectSchema[Connection](
		"Connection",
		map[string]*schema.PropertySchema{
			"host": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), regexp.MustCompile("^[a-z0-9./:_-]+$")),
				schema.NewDisplayValue(
					schema.PointerTo("Host"),
					schema.PointerTo("Host name for Dockerd."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(mustJSONEncode(getDefaultDockerSocket())),
				[]string{
					"'unix:///var/run/docker.sock'",
					"'npipe:////./pipe/docker_engine'",
				},
			),
			"cacert": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^-----BEGIN CERTIFICATE-----\n.*\n-----END CERTIFICATE-----\\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("CA certificate"),
					schema.PointerTo("CA certificate in PEM format to verify the Dockerd server certificate against."),
					nil,
				),
				false,
				[]string{"cert", "key"},
				nil,
				nil,
				nil,
				[]string{
					mustJSONEncode(base64Decode(`LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUI0VENDQVl1Z0F3SUJBZ0lVQ0hoaGZmWTFsemV6R2F0WU1SMDJncEVKQ2hrd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TWpBNU1qZ3dOVEk0TVRKYUZ3MHlNekE1Ck1qZ3dOVEk0TVRKYU1FVXhDekFKQmdOVkJBWVRBa0ZWTVJNd0VRWURWUVFJREFwVGIyMWxMVk4wWVhSbE1TRXcKSHdZRFZRUUtEQmhKYm5SbGNtNWxkQ0JYYVdSbmFYUnpJRkIwZVNCTWRHUXdYREFOQmdrcWhraUc5dzBCQVFFRgpBQU5MQURCSUFrRUFycjg5ZjJrZ2dTTy95YUNCNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxCm5FaVBuTGJ6RERnTVU4S0NQQU1oSTdKcFlSbEhuaXB4V3dJREFRQUJvMU13VVRBZEJnTlZIUTRFRmdRVWlaNkoKRHd1RjlRQ2gxdndRR1hzMk11dHVROUV3SHdZRFZSMGpCQmd3Rm9BVWlaNkpEd3VGOVFDaDF2d1FHWHMyTXV0dQpROUV3RHdZRFZSMFRBUUgvQkFVd0F3RUIvekFOQmdrcWhraUc5dzBCQVFzRkFBTkJBRllJRk0yN0JEaUc3MjVkClZraFJibGt2WnplUkhoY3d0RE9RVEM5ZDhNL0x5bU4yeTBuSFNsSkNabS9Mby9hSDh2aVNZMXZpMUdTSGZEejcKVGxmZThncz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=`)),
				},
			),
			"cert": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^-----BEGIN CERTIFICATE-----\n.*\n-----END CERTIFICATE-----\\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("Client certificate"),
					schema.PointerTo("Client certificate in PEM format to authenticate against the Dockerd with."),
					nil,
				),
				false,
				[]string{"key"},
				nil,
				nil,
				nil,
				[]string{
					mustJSONEncode(base64Decode(`LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUI0VENDQVl1Z0F3SUJBZ0lVQ0hoaGZmWTFsemV6R2F0WU1SMDJncEVKQ2hrd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TWpBNU1qZ3dOVEk0TVRKYUZ3MHlNekE1Ck1qZ3dOVEk0TVRKYU1FVXhDekFKQmdOVkJBWVRBa0ZWTVJNd0VRWURWUVFJREFwVGIyMWxMVk4wWVhSbE1TRXcKSHdZRFZRUUtEQmhKYm5SbGNtNWxkQ0JYYVdSbmFYUnpJRkIwZVNCTWRHUXdYREFOQmdrcWhraUc5dzBCQVFFRgpBQU5MQURCSUFrRUFycjg5ZjJrZ2dTTy95YUNCNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxCm5FaVBuTGJ6RERnTVU4S0NQQU1oSTdKcFlSbEhuaXB4V3dJREFRQUJvMU13VVRBZEJnTlZIUTRFRmdRVWlaNkoKRHd1RjlRQ2gxdndRR1hzMk11dHVROUV3SHdZRFZSMGpCQmd3Rm9BVWlaNkpEd3VGOVFDaDF2d1FHWHMyTXV0dQpROUV3RHdZRFZSMFRBUUgvQkFVd0F3RUIvekFOQmdrcWhraUc5dzBCQVFzRkFBTkJBRllJRk0yN0JEaUc3MjVkClZraFJibGt2WnplUkhoY3d0RE9RVEM5ZDhNL0x5bU4yeTBuSFNsSkNabS9Mby9hSDh2aVNZMXZpMUdTSGZEejcKVGxmZThncz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=`)),
				},
			),
			"key": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^-----BEGIN ([A-Z]+) PRIVATE KEY-----\n.*\n-----END ([A-Z]+) PRIVATE KEY-----\\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("Client key"),
					schema.PointerTo("Client private key in PEM format to authenticate against the Dockerd with."),
					nil,
				),
				false,
				[]string{"cert"},
				nil,
				nil,
				nil,
				[]string{
					mustJSONEncode(base64Decode(`LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUJWQUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQVQ0d2dnRTZBZ0VBQWtFQXJyODlmMmtnZ1NPL3lhQ0IKNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxbkVpUG5MYnpERGdNVThLQ1BBTWhJN0pwWVJsSApuaXB4V3dJREFRQUJBa0J5YnUveDBNRWxjR2kydS9KMlVkd1Njc1Y3amU1VHQxMno4Mmw3VEptWkZGSjhSTG1jCnJoMDBHdmViNFZwR2hkMStjM2xaYk8xbUlUNnYzdkhNOUEwaEFpRUExNEVXNmIrOTlYWXphNys1dXdJRHVpTSsKQnozcGtLKzl0bGZWWEU3SnlLc0NJUURQbFlKNXh0YnVUK1Z2QjNYT2REL1ZXaUVxRW12RTNmbFYwNDE3UnFoYQpFUUlnYnl4d05wd3RFZ0V0Vzh1bnRCckE4M2lVMmtXTlJZL3o3YXA0TGt1Uyswc0NJR2UyRSswUm1mcVFzbGxwCmljTXZNMkU5MllueWtDTlluNlR3d0NRU0pqUnhBaUVBbzlNbWFWbEs3WWRoU01QbzUydUpZemQ5TVFaSnFocSsKbEIxWkdEeC9BUkU9Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K`)),
				},
			),
		},
	),
	schema.NewStructMappedObjectSchema[Deployment](
		"Deployment",
		map[string]*schema.PropertySchema{
			"container": schema.NewPropertySchema(
				schema.NewRefSchema("ContainerConfig", nil),
				schema.NewDisplayValue(schema.PointerTo("Container configuration"), schema.PointerTo("Provides information about the container for the plugin."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"host": schema.NewPropertySchema(
				schema.NewRefSchema("HostConfig", nil),
				schema.NewDisplayValue(schema.PointerTo("Host configuration"), schema.PointerTo("Provides information about the container host for the plugin."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"network": schema.NewPropertySchema(
				schema.NewRefSchema("NetworkConfig", nil),
				schema.NewDisplayValue(schema.PointerTo("Network configuration"), schema.PointerTo("Provides information about the container networking for the plugin."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"platform": schema.NewPropertySchema(
				schema.NewRefSchema("PlatformConfig", nil),
				schema.NewDisplayValue(schema.PointerTo("Platform configuration"), schema.PointerTo("Provides information about the container host platform for the plugin."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"imagePullPolicy": schema.NewPropertySchema(
				schema.NewStringEnumSchema(map[string]string{
					string(ImagePullPolicyAlways):       "Always",
					string(ImagePullPolicyIfNotPresent): "If not present",
					string(ImagePullPolicyNever):        "Never",
				}),
				schema.NewDisplayValue(schema.PointerTo("Image pull policy"), schema.PointerTo("When to pull the plugin image."), nil),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(mustJSONEncode(string(ImagePullPolicyIfNotPresent))),
				nil,
			),
		},
	),
	schema.NewStructMappedObjectSchema[*container.Config](
		"ContainerConfig",
		map[string]*schema.PropertySchema{
			"Hostname": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), regexp.MustCompile("^[a-zA-Z0-9-_.]+$")),
				schema.NewDisplayValue(schema.PointerTo("Hostname"), schema.PointerTo("Hostname for the plugin container."), nil),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(mustJSONEncode(string(ImagePullPolicyIfNotPresent))),
				nil,
			),
			"Domainname": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), regexp.MustCompile("^[a-zA-Z0-9-_.]+$")),
				schema.NewDisplayValue(schema.PointerTo("Domain name"), schema.PointerTo("Domain name for the plugin container."), nil),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(mustJSONEncode(string(ImagePullPolicyIfNotPresent))),
				nil,
			),
			"User": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), regexp.MustCompile("^[a-z_][a-z0-9_-]*[$]?(:[a-z_][a-z0-9_-]*[$]?)$")),
				schema.NewDisplayValue(schema.PointerTo("Username"), schema.PointerTo("User that will run the command inside the container. Optionally, a group can be specified in the user:group format."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"Env": schema.NewPropertySchema(
				schema.NewMapSchema(
					schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(255), regexp.MustCompile("^[A-Z0-9_]+$")),
					schema.NewStringSchema(nil, schema.IntPointer(32760), nil),
					nil,
					nil,
				),
				schema.NewDisplayValue(schema.PointerTo("Environment variables"), schema.PointerTo("Environment variables to set on the plugin container."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"NetworkDisabled": schema.NewPropertySchema(
				schema.NewBoolSchema(),
				schema.NewDisplayValue(schema.PointerTo("Disable network"), schema.PointerTo("Disable container networking completely."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"MacAddress": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, regexp.MustCompile("^[a-fA-F0-9]{2}(:[a-fA-F0-9]{2}){5}$")),
				schema.NewDisplayValue(schema.PointerTo("MAC address"), schema.PointerTo("Media Access Control address for the container."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
		},
	),
	schema.NewStructMappedObjectSchema[*container.HostConfig](
		"HostConfig",
		map[string]*schema.PropertySchema{
			"NetworkMode": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, regexp.MustCompile("^(none|bridge|host|container:[a-zA-Z0-9][a-zA-Z0-9_.-]+|[a-zA-Z0-9][a-zA-Z0-9_.-]+)$")),
				schema.NewDisplayValue(schema.PointerTo("Network mode"), schema.PointerTo("Specifies either the network mode, the container network to attach to, or a name of a Docker network to use."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				[]string{
					mustJSONEncode("none"),
					mustJSONEncode("bridge"),
					mustJSONEncode("host"),
					mustJSONEncode("container:container-name"),
					mustJSONEncode("network-name"),
				},
			),
			"PortBindings": schema.NewPropertySchema(
				schema.NewMapSchema(
					schema.NewStringSchema(nil, nil, regexp.MustCompile("^[0-9]+(/[a-zA-Z0-9]+)$")),
					schema.NewListSchema(
						schema.NewRefSchema("PortBinding", nil),
						nil,
						nil,
					),
					nil,
					nil,
				),
				schema.NewDisplayValue(schema.PointerTo("Port bindings"), schema.PointerTo("Ports to expose on the host machine. Ports are specified in the format of portnumber/protocol."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"CapAdd": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("Add capabilities"), schema.PointerTo("Add capabilities to the container."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"CapDrop": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("Drop capabilities"), schema.PointerTo("Drop capabilities from the container."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"CgroupnsMode": schema.NewPropertySchema(
				schema.NewStringEnumSchema(map[string]string{
					"private": "Private",
					"host":    "Host",
					"":        "Empty",
				}),
				schema.NewDisplayValue(schema.PointerTo("CGroup namespace mode"), schema.PointerTo("CGroup namespace mode to use for the container."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"Dns": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("DNS servers"), schema.PointerTo("DNS servers to use for lookup."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"DnsOptions": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("DNS options"), schema.PointerTo("DNS options to look for."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"DnsSearch": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("DNS search"), schema.PointerTo("DNS search domain."), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"ExtraHosts": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
				schema.NewDisplayValue(schema.PointerTo("Extra hosts"), schema.PointerTo("Extra hosts entries to add"), nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
		},
	),
	schema.NewStructMappedObjectSchema[*network.NetworkingConfig](
		"NetworkConfig",
		map[string]*schema.PropertySchema{},
	),
	schema.NewStructMappedObjectSchema[*specs.Platform](
		"PlatformConfig",
		map[string]*schema.PropertySchema{},
	),
	schema.NewStructMappedObjectSchema[*nat.PortBinding](
		"PortBinding",
		map[string]*schema.PropertySchema{
			"HostIP": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(schema.PointerTo("Host IP"), nil, nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"HostPort": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, regexp.MustCompile("^0-9+$")),
				schema.NewDisplayValue(schema.PointerTo("Host port"), nil, nil),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
		},
	),
)

// Config is the configuration structure of the Docker connector.
type Config struct {
	Connection Connection `json:"connection"`
	Deployment Deployment `json:"deployment"`
	Timeouts   Timeouts   `json:"timeouts"`
}

// Connection describes how to connect to the Docker daemon.
type Connection struct {
	Host   string `json:"host"`
	CACert string `json:"cacert"`
	Cert   string `json:"cert"`
	Key    string `json:"key"`
}

// ImagePullPolicy drives when an image should be pulled.
type ImagePullPolicy string

const (
	// ImagePullPolicyAlways means that the container image will be pulled for every workflow run.
	ImagePullPolicyAlways ImagePullPolicy = "Always"
	// ImagePullPolicyIfNotPresent means the image will be pulled if the image is not present locally, an empty tag, or
	// the "latest" tag was specified.
	ImagePullPolicyIfNotPresent ImagePullPolicy = "IfNotPresent"
	// ImagePullPolicyNever means that the image will never be pulled, and if the image is not available locally the
	// execution will fail.
	ImagePullPolicyNever ImagePullPolicy = "Never"
)

// Deployment contains the information about deploying the plugin.
type Deployment struct {
	ContainerConfig *container.Config         `json:"container"`
	HostConfig      *container.HostConfig     `json:"host"`
	NetworkConfig   *network.NetworkingConfig `json:"network"`
	Platform        *specs.Platform           `json:"platform"`

	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy"`
}

// Timeouts drive the timeouts for various interactions in relation to Docker.
type Timeouts struct {
	HTTP time.Duration `json:"http"`
}

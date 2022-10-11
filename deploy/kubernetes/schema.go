package kubernetes

import (
	"regexp"
	"time"

	"go.flow.arcalot.io/engine/internal/util"
	"go.flow.arcalot.io/pluginsdk/schema"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// region Container properties

var containerNameProperty = schema.NewPropertySchema(
	dnsSubdomainName,
	schema.NewDisplayValue(
		schema.PointerTo("Name"),
		schema.PointerTo(
			"Name for the container. Each container in a pod must have a unique name.",
		),
		nil,
	),
	true,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerImageProperty = schema.NewPropertySchema(
	imageTag,
	schema.NewDisplayValue(
		schema.PointerTo("Image"),
		schema.PointerTo(
			"Container image to use for this container.",
		),
		nil,
	),
	true,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerCommandProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewStringSchema(nil, nil, nil),
		schema.IntPointer(1),
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Command"),
		schema.PointerTo(
			"Override container entry point. Not executed with a shell.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerArgsProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewStringSchema(nil, nil, nil),
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Arguments"),
		schema.PointerTo(
			"Arguments to the entypoint (command).",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerWorkingDirProperty = schema.NewPropertySchema(
	schema.NewStringSchema(nil, nil, nil),
	schema.NewDisplayValue(
		schema.PointerTo("Working directory"),
		schema.PointerTo(
			"Override the container working directory.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerEnvFromProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewRefSchema("EnvFromSource", nil),
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Environment sources"),
		schema.PointerTo(
			"List of sources to populate the environment variables from.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerEnvProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewStructMappedObjectSchema[v1.EnvVar](
			"EnvVar",
			map[string]*schema.PropertySchema{
				"name": schema.NewPropertySchema(
					identifier,
					schema.NewDisplayValue(
						schema.PointerTo("Name"),
						schema.PointerTo(
							"Environment variables name.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"value": schema.NewPropertySchema(
					schema.NewStringSchema(nil, nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Value"),
						schema.PointerTo(
							"Value for the environment variable.",
						),
						nil,
					),
					false,
					nil,
					[]string{"valueFrom"},
					[]string{"valueFrom"},
					nil,
					nil,
				).TreatEmptyAsDefaultValue(),
				"valueFrom": schema.NewPropertySchema(
					schema.NewRefSchema("EnvFromSource", nil),
					schema.NewDisplayValue(
						schema.PointerTo("Value source"),
						schema.PointerTo(
							"Load the environment variable from a secret or config map.",
						),
						nil,
					),
					false,
					nil,
					[]string{"value"},
					[]string{"value"},
					nil,
					nil,
				),
			},
		),
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Environment"),
		schema.PointerTo(
			"Environment variables for this container.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerVolumeMountsProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewStructMappedObjectSchema[v1.VolumeMount](
			"VolumeMount",
			map[string]*schema.PropertySchema{
				"name": schema.NewPropertySchema(
					schema.NewStringSchema(schema.IntPointer(1), nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Volume name"),
						schema.PointerTo(
							"Must match the pod volume to mount.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"readOnly": schema.NewPropertySchema(
					schema.NewBoolSchema(),
					schema.NewDisplayValue(
						schema.PointerTo("Read only"),
						schema.PointerTo(
							"Mount volume as read-only.",
						),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					schema.PointerTo(`false`),
					nil,
				),
				"mountPath": schema.NewPropertySchema(
					schema.NewStringSchema(schema.IntPointer(1), nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Mount path"),
						schema.PointerTo(
							"Path to mount the volume on inside the container.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"subPath": schema.NewPropertySchema(
					schema.NewStringSchema(schema.IntPointer(1), nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Subpath"),
						schema.PointerTo(
							"Path from the volume to mount.",
						),
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
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Volume mounts"),
		schema.PointerTo(
			"Pod volumes to mount on this container.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerVolumeDevicesProperty = schema.NewPropertySchema(
	schema.NewListSchema(
		schema.NewStructMappedObjectSchema[v1.VolumeDevice](
			"VolumeDevice",
			map[string]*schema.PropertySchema{
				"name": schema.NewPropertySchema(
					schema.NewStringSchema(schema.IntPointer(1), nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Name"),
						schema.PointerTo(
							"Must match the persistent volume claim in the pod.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"devicePath": schema.NewPropertySchema(
					schema.NewStringSchema(schema.IntPointer(1), nil, nil),
					schema.NewDisplayValue(
						schema.PointerTo("Device path"),
						schema.PointerTo(
							"Path inside the container the device will be mapped to.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
			},
		),
		nil,
		nil,
	),
	schema.NewDisplayValue(
		schema.PointerTo("Volume device"),
		schema.PointerTo(
			"Mount a raw block device within the container.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)
var containerImagePullPolicyProperty = schema.NewPropertySchema(
	schema.NewStringEnumSchema(
		map[string]*schema.DisplayValue{
			string(v1.PullAlways):       {NameValue: schema.PointerTo("Always")},
			string(v1.PullNever):        {NameValue: schema.PointerTo("Never")},
			string(v1.PullIfNotPresent): {NameValue: schema.PointerTo("If not present")},
		},
	),
	schema.NewDisplayValue(
		schema.PointerTo("Volume device"),
		schema.PointerTo(
			"Mount a raw block device within the container.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	schema.PointerTo(`"IfNotPresent"`),
	nil,
).TreatEmptyAsDefaultValue()
var containerSecurityContextPorperty = schema.NewPropertySchema(
	schema.NewStructMappedObjectSchema[*v1.SecurityContext](
		"SecurityContext",
		map[string]*schema.PropertySchema{
			"capabilities": schema.NewPropertySchema(
				schema.NewStructMappedObjectSchema[v1.Capabilities](
					"Capabilities",
					map[string]*schema.PropertySchema{
						"add": schema.NewPropertySchema(
							schema.NewListSchema(
								schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^[A-Z_]+$`)),
								nil,
								nil,
							),
							schema.NewDisplayValue(
								schema.PointerTo("Add"),
								schema.PointerTo(
									"Add POSIX capabilities.",
								),
								nil,
							),
							false,
							nil,
							nil,
							nil,
							nil,
							nil,
						),
						"drop": schema.NewPropertySchema(
							schema.NewListSchema(
								schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^[A-Z_]+$`)),
								nil,
								nil,
							),
							schema.NewDisplayValue(
								schema.PointerTo("Drop"),
								schema.PointerTo(
									"Drop POSIX capabilities.",
								),
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
				schema.NewDisplayValue(
					schema.PointerTo("Capabilities"),
					schema.PointerTo(
						"Add or drop POSIX capabilities.",
					),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"privileged": schema.NewPropertySchema(
				schema.NewBoolSchema(),
				schema.NewDisplayValue(
					schema.PointerTo("Privileged"),
					schema.PointerTo(
						"Run the container in privileged mode.",
					),
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
	schema.NewDisplayValue(
		schema.PointerTo("Volume device"),
		schema.PointerTo(
			"Mount a raw block device within the container.",
		),
		nil,
	),
	false,
	nil,
	nil,
	nil,
	nil,
	nil,
)

// endregion

// Schema describes the schema for Kubernetes deployments.
var Schema = schema.NewTypedScopeSchema[*Config](
	// region Config
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
			"pod": schema.NewPropertySchema(
				schema.NewRefSchema("Pod", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Pod"),
					schema.PointerTo("Pod configuration for the plugin."),
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
	// endregion
	// region Timeouts
	schema.NewStructMappedObjectSchema[Timeouts](
		"Timeouts",
		map[string]*schema.PropertySchema{
			"http": schema.NewPropertySchema(
				schema.NewIntSchema(schema.PointerTo(int64(100*time.Millisecond)), nil, schema.UnitDurationNanoseconds),
				schema.NewDisplayValue(
					schema.PointerTo("HTTP"),
					schema.PointerTo("HTTP timeout for the Docker API."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(util.JSONEncode("15s")),
				nil,
			).TreatEmptyAsDefaultValue(),
		},
	),
	// endregion
	// region Connection
	schema.NewStructMappedObjectSchema[Connection](
		"Connection",
		map[string]*schema.PropertySchema{
			"host": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Host"),
					schema.PointerTo("Host name and port of the Kubernetes server"),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(`"kubernetes.default.svc"`),
				nil,
			).TreatEmptyAsDefaultValue(),
			"path": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Path"),
					schema.PointerTo("Path to the API server."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(`"/api"`),
				nil,
			).TreatEmptyAsDefaultValue(),
			"username": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Username"),
					schema.PointerTo("Username for basic authentication."),
					nil,
				),
				false,
				[]string{"password"},
				nil,
				nil,
				nil,
				nil,
			),
			"password": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Password"),
					schema.PointerTo("Password for basic authentication."),
					nil,
				),
				false,
				[]string{"username"},
				nil,
				nil,
				nil,
				nil,
			),
			"serverName": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("TLS server name"),
					schema.PointerTo("Expected TLS server name to verify in the certificate."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"cacert": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^\s*-----BEGIN CERTIFICATE-----(\s*.*\s*)*-----END CERTIFICATE-----\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("CA certificate"),
					schema.PointerTo("CA certificate in PEM format to verify Kubernetes server certificate against."),
					nil,
				),
				false,
				[]string{"cert", "key"},
				nil,
				nil,
				nil,
				[]string{
					util.JSONEncode(util.Base64Decode(`LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUI0VENDQVl1Z0F3SUJBZ0lVQ0hoaGZmWTFsemV6R2F0WU1SMDJncEVKQ2hrd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TWpBNU1qZ3dOVEk0TVRKYUZ3MHlNekE1Ck1qZ3dOVEk0TVRKYU1FVXhDekFKQmdOVkJBWVRBa0ZWTVJNd0VRWURWUVFJREFwVGIyMWxMVk4wWVhSbE1TRXcKSHdZRFZRUUtEQmhKYm5SbGNtNWxkQ0JYYVdSbmFYUnpJRkIwZVNCTWRHUXdYREFOQmdrcWhraUc5dzBCQVFFRgpBQU5MQURCSUFrRUFycjg5ZjJrZ2dTTy95YUNCNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxCm5FaVBuTGJ6RERnTVU4S0NQQU1oSTdKcFlSbEhuaXB4V3dJREFRQUJvMU13VVRBZEJnTlZIUTRFRmdRVWlaNkoKRHd1RjlRQ2gxdndRR1hzMk11dHVROUV3SHdZRFZSMGpCQmd3Rm9BVWlaNkpEd3VGOVFDaDF2d1FHWHMyTXV0dQpROUV3RHdZRFZSMFRBUUgvQkFVd0F3RUIvekFOQmdrcWhraUc5dzBCQVFzRkFBTkJBRllJRk0yN0JEaUc3MjVkClZraFJibGt2WnplUkhoY3d0RE9RVEM5ZDhNL0x5bU4yeTBuSFNsSkNabS9Mby9hSDh2aVNZMXZpMUdTSGZEejcKVGxmZThncz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=`)),
				},
			),
			"cert": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^\s*-----BEGIN CERTIFICATE-----(\s*.*\s*)*-----END CERTIFICATE-----\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("Client certificate"),
					schema.PointerTo("Client certificate in PEM format to authenticate against Kubernetes with."),
					nil,
				),
				false,
				[]string{"key"},
				nil,
				nil,
				nil,
				[]string{
					util.JSONEncode(util.Base64Decode(`LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUI0VENDQVl1Z0F3SUJBZ0lVQ0hoaGZmWTFsemV6R2F0WU1SMDJncEVKQ2hrd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TWpBNU1qZ3dOVEk0TVRKYUZ3MHlNekE1Ck1qZ3dOVEk0TVRKYU1FVXhDekFKQmdOVkJBWVRBa0ZWTVJNd0VRWURWUVFJREFwVGIyMWxMVk4wWVhSbE1TRXcKSHdZRFZRUUtEQmhKYm5SbGNtNWxkQ0JYYVdSbmFYUnpJRkIwZVNCTWRHUXdYREFOQmdrcWhraUc5dzBCQVFFRgpBQU5MQURCSUFrRUFycjg5ZjJrZ2dTTy95YUNCNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxCm5FaVBuTGJ6RERnTVU4S0NQQU1oSTdKcFlSbEhuaXB4V3dJREFRQUJvMU13VVRBZEJnTlZIUTRFRmdRVWlaNkoKRHd1RjlRQ2gxdndRR1hzMk11dHVROUV3SHdZRFZSMGpCQmd3Rm9BVWlaNkpEd3VGOVFDaDF2d1FHWHMyTXV0dQpROUV3RHdZRFZSMFRBUUgvQkFVd0F3RUIvekFOQmdrcWhraUc5dzBCQVFzRkFBTkJBRllJRk0yN0JEaUc3MjVkClZraFJibGt2WnplUkhoY3d0RE9RVEM5ZDhNL0x5bU4yeTBuSFNsSkNabS9Mby9hSDh2aVNZMXZpMUdTSGZEejcKVGxmZThncz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=`)),
				},
			),
			"key": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile(`^\s*-----BEGIN ([A-Z]+) PRIVATE KEY-----(\s*.*\s*)*-----END ([A-Z]+) PRIVATE KEY-----\s*$`)),
				schema.NewDisplayValue(
					schema.PointerTo("Client key"),
					schema.PointerTo("Client private key in PEM format to authenticate against Kubernetes with."),
					nil,
				),
				false,
				[]string{"cert"},
				nil,
				nil,
				nil,
				[]string{
					util.JSONEncode(util.Base64Decode(`LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUJWQUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQVQ0d2dnRTZBZ0VBQWtFQXJyODlmMmtnZ1NPL3lhQ0IKNkV3SVFlVDZacHRCb1gwWnZDTUkrRHBrQ3dxT1M1ZndSYmoxbkVpUG5MYnpERGdNVThLQ1BBTWhJN0pwWVJsSApuaXB4V3dJREFRQUJBa0J5YnUveDBNRWxjR2kydS9KMlVkd1Njc1Y3amU1VHQxMno4Mmw3VEptWkZGSjhSTG1jCnJoMDBHdmViNFZwR2hkMStjM2xaYk8xbUlUNnYzdkhNOUEwaEFpRUExNEVXNmIrOTlYWXphNys1dXdJRHVpTSsKQnozcGtLKzl0bGZWWEU3SnlLc0NJUURQbFlKNXh0YnVUK1Z2QjNYT2REL1ZXaUVxRW12RTNmbFYwNDE3UnFoYQpFUUlnYnl4d05wd3RFZ0V0Vzh1bnRCckE4M2lVMmtXTlJZL3o3YXA0TGt1Uyswc0NJR2UyRSswUm1mcVFzbGxwCmljTXZNMkU5MllueWtDTlluNlR3d0NRU0pqUnhBaUVBbzlNbWFWbEs3WWRoU01QbzUydUpZemQ5TVFaSnFocSsKbEIxWkdEeC9BUkU9Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K`)),
				},
			),
			"bearerToken": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Bearer token"),
					schema.PointerTo("Bearer token to authenticate against the Kubernetes API with."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"qps": schema.NewPropertySchema(
				schema.NewFloatSchema(
					schema.PointerTo(0.0),
					nil,
					schema.NewUnits(schema.NewUnit(
						"q",
						"q",
						"query",
						"queries",
					), nil)),
				schema.NewDisplayValue(
					schema.PointerTo("QPS"),
					schema.PointerTo("Queries Per Second allowed against the API."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(`5.0`),
				nil,
			).TreatEmptyAsDefaultValue(),
			"burst": schema.NewPropertySchema(
				schema.NewIntSchema(schema.IntPointer(0), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Burst"),
					schema.PointerTo("Burst value for query throttling."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo(`10`),
				nil,
			).TreatEmptyAsDefaultValue(),
		},
	),
	// endregion
	// region Pod
	schema.NewStructMappedObjectSchema[Pod](
		"Pod",
		map[string]*schema.PropertySchema{
			"metadata": schema.NewPropertySchema(
				schema.NewRefSchema("ObjectMeta", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Metadata"),
					schema.PointerTo("Pod metadata."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"spec": schema.NewPropertySchema(
				schema.NewRefSchema("PodSpec", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Specification"),
					schema.PointerTo("Pod specification."),
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
	// endregion
	// region ObjectMeta
	schema.NewStructMappedObjectSchema[metav1.ObjectMeta](
		"ObjectMeta",
		map[string]*schema.PropertySchema{
			"name": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Name"),
					schema.PointerTo("Pod name."),
					nil,
				),
				false,
				nil,
				nil,
				[]string{
					"generateName",
				},
				nil,
				nil,
			).TreatEmptyAsDefaultValue(),
			"generateName": schema.NewPropertySchema(
				schema.NewStringSchema(nil, nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Name prefix"),
					schema.PointerTo("Name prefix to generate pod names from."),
					nil,
				),
				false,
				nil,
				nil,
				[]string{
					"name",
				},
				nil,
				nil,
			).TreatEmptyAsDefaultValue(),
			"namespace": schema.NewPropertySchema(
				dnsSubdomainName,
				schema.NewDisplayValue(
					schema.PointerTo("Namespace"),
					schema.PointerTo("Kubernetes namespace to deploy in."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				schema.PointerTo("\"default\""),
				nil,
			).TreatEmptyAsDefaultValue(),
			"labels": schema.NewPropertySchema(
				schema.NewMapSchema(
					labelName,
					labelValue,
					nil,
					nil,
				),
				schema.NewDisplayValue(
					schema.PointerTo("Labels"),
					schema.PointerTo(
						"Kubernetes labels to appy. See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ for details.",
					),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"annotations": schema.NewPropertySchema(
				schema.NewMapSchema(
					labelName,
					labelValue,
					nil,
					nil,
				),
				schema.NewDisplayValue(
					schema.PointerTo("Annotations"),
					schema.PointerTo(
						"Kubernetes annotations to appy. See https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/ for details.",
					),
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
	// endregion
	// region PodSpec
	schema.NewStructMappedObjectSchema[PodSpec](
		"PodSpec",
		map[string]*schema.PropertySchema{
			"volumes": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewRefSchema("Volume", nil), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Volumes"),
					schema.PointerTo(
						"A list of volumes that can be mounted by containers belonging to the pod.",
					),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"initContainers": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewRefSchema("Container", nil), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Init containers"),
					schema.PointerTo(
						"A list of initialization containers belonging to the pod.",
					),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"containers": schema.NewPropertySchema(
				schema.NewListSchema(schema.NewRefSchema("Container", nil), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Containers"),
					schema.PointerTo(
						"A list of containers belonging to the pod.",
					),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"pluginContainer": schema.NewPropertySchema(
				schema.NewStructMappedObjectSchema[v1.Container](
					"Plugin container",
					map[string]*schema.PropertySchema{
						"name": schema.NewPropertySchema(
							dnsSubdomainName,
							schema.NewDisplayValue(
								schema.PointerTo("Name"),
								schema.PointerTo(
									"Name for the container. Each container in a pod must have a unique name.",
								),
								nil,
							),
							false,
							nil,
							nil,
							nil,
							schema.PointerTo(`"arcaflow-plugin-container"`),
							nil,
						).TreatEmptyAsDefaultValue(),
						"envFrom":         containerEnvFromProperty,
						"env":             containerEnvProperty,
						"volumeMounts":    containerVolumeMountsProperty,
						"volumeDevices":   containerVolumeDevicesProperty,
						"imagePullPolicy": containerImagePullPolicyProperty,
						"securityContext": containerSecurityContextPorperty,
					},
				),
				schema.NewDisplayValue(
					schema.PointerTo("Plugin container"),
					schema.PointerTo(
						"The container to run the plugin in.",
					),
					nil,
				),
				true,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
		},
	),
	// endregion
	// region Container
	schema.NewStructMappedObjectSchema[v1.Container](
		"Container",
		map[string]*schema.PropertySchema{
			"name":            containerNameProperty,
			"image":           containerImageProperty,
			"command":         containerCommandProperty,
			"args":            containerArgsProperty,
			"workingDir":      containerWorkingDirProperty,
			"envFrom":         containerEnvFromProperty,
			"env":             containerEnvProperty,
			"volumeMounts":    containerVolumeMountsProperty,
			"volumeDevices":   containerVolumeDevicesProperty,
			"imagePullPolicy": containerImagePullPolicyProperty,
			"securityContext": containerSecurityContextPorperty,
		},
	),
	// endregion
	// region EnvFromSource
	schema.NewStructMappedObjectSchema[v1.EnvFromSource](
		"EnvFromSource",
		map[string]*schema.PropertySchema{
			"prefix": schema.NewPropertySchema(
				identifier,
				schema.NewDisplayValue(
					schema.PointerTo("Prefix"),
					schema.PointerTo("An optional identifier to prepend to each key in the ConfigMap."),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"configMapRef": schema.NewPropertySchema(
				schema.NewStructMappedObjectSchema[v1.ConfigMapEnvSource](
					"ConfigMapEnvSource",
					map[string]*schema.PropertySchema{
						"name": schema.NewPropertySchema(
							schema.NewStringSchema(schema.IntPointer(1), nil, nil),
							schema.NewDisplayValue(
								schema.PointerTo("Name"),
								schema.PointerTo("Name of the referenced config map."),
								nil,
							),
							true,
							nil,
							nil,
							nil,
							nil,
							nil,
						),
						"optional": schema.NewPropertySchema(
							schema.NewBoolSchema(),
							schema.NewDisplayValue(
								schema.PointerTo("Optional"),
								schema.PointerTo("Specify whether the config map must be defined."),
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
				schema.NewDisplayValue(
					schema.PointerTo("Config map source"),
					schema.PointerTo("Populates the source from a config map."),
					nil,
				),
				false,
				nil,
				[]string{"secretRef"},
				[]string{"secretRef"},
				nil,
				nil,
			),
			"secretRef": schema.NewPropertySchema(
				schema.NewStructMappedObjectSchema[v1.ConfigMapEnvSource](
					"ConfigMapEnvSource",
					map[string]*schema.PropertySchema{
						"name": schema.NewPropertySchema(
							schema.NewStringSchema(schema.IntPointer(1), nil, nil),
							schema.NewDisplayValue(
								schema.PointerTo("Name"),
								schema.PointerTo("Name of the referenced secret."),
								nil,
							),
							true,
							nil,
							nil,
							nil,
							nil,
							nil,
						),
						"optional": schema.NewPropertySchema(
							schema.NewBoolSchema(),
							schema.NewDisplayValue(
								schema.PointerTo("Optional"),
								schema.PointerTo("Specify whether the secret must be defined."),
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
				schema.NewDisplayValue(
					schema.PointerTo("Secret source"),
					schema.PointerTo("Populates the source from a secret."),
					nil,
				),
				false,
				nil,
				[]string{"configMapRef"},
				[]string{"configMapRef"},
				nil,
				nil,
			),
		},
	),
	// endregion
	// region Volume
	schema.NewStructMappedObjectSchema[v1.Volume](
		"Volume",
		map[string]*schema.PropertySchema{
			"name": schema.NewPropertySchema(
				dnsSubdomainName,
				schema.NewDisplayValue(
					schema.PointerTo("Name"),
					schema.PointerTo("The name this volume can be referenced by."),
					nil,
				),
				true,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			"hostPath": schema.NewPropertySchema(
				schema.NewRefSchema("HostPathVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Host path"),
					schema.PointerTo("Mount volume from the host."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("hostPath"),
				generateVolumeTypeList("hostPath"),
				nil,
				nil,
			),
			"emptyDir": schema.NewPropertySchema(
				schema.NewRefSchema("EmptyDirVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Empty directory"),
					schema.PointerTo("Temporary empty directory."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("emptyDir"),
				generateVolumeTypeList("emptyDir"),
				nil,
				nil,
			),
			"gcePersistentDisk": schema.NewPropertySchema(
				schema.NewRefSchema("GCEPersistentDiskVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("GCE disk"),
					schema.PointerTo("Google Cloud disk."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("gcePersistentDisk"),
				generateVolumeTypeList("gcePersistentDisk"),
				nil,
				nil,
			),
			"awsElasticBlockStore": schema.NewPropertySchema(
				schema.NewRefSchema("AWSElasticBlockStoreVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("AWS EBS"),
					schema.PointerTo("AWS Elastic Block Storage."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("awsElasticBlockStore"),
				generateVolumeTypeList("awsElasticBlockStore"),
				nil,
				nil,
			),
			"secret": schema.NewPropertySchema(
				schema.NewRefSchema("SecretVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Secret"),
					schema.PointerTo("Mount a Kubernetes secret."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("secret"),
				generateVolumeTypeList("secret"),
				nil,
				nil,
			),
			"nfs": schema.NewPropertySchema(
				schema.NewRefSchema("NFSVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("NFS"),
					schema.PointerTo("Mount an NFS share."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("nfs"),
				generateVolumeTypeList("nfs"),
				nil,
				nil,
			),
			"iscsi": schema.NewPropertySchema(
				schema.NewRefSchema("ISCSIVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("iSCSI"),
					schema.PointerTo("Mount an iSCSI volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("iscsi"),
				generateVolumeTypeList("iscsi"),
				nil,
				nil,
			),
			"glusterfs": schema.NewPropertySchema(
				schema.NewRefSchema("GlusterfsVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("GlusterFS"),
					schema.PointerTo("Mount a Gluster volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("glusterfs"),
				generateVolumeTypeList("glusterfs"),
				nil,
				nil,
			),
			"persistentVolumeClaim": schema.NewPropertySchema(
				schema.NewRefSchema("PersistentVolumeClaimVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Persistent Volume Claim"),
					schema.PointerTo("Mount a Persistent Volume Claim."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("persistentVolumeClaim"),
				generateVolumeTypeList("persistentVolumeClaim"),
				nil,
				nil,
			),
			"rbd": schema.NewPropertySchema(
				schema.NewRefSchema("RBDVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Rados Block Device"),
					schema.PointerTo("Mount a Rados Block Device."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("rbd"),
				generateVolumeTypeList("rbd"),
				nil,
				nil,
			),
			"flexVolume": schema.NewPropertySchema(
				schema.NewRefSchema("FlexVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Flex"),
					schema.PointerTo("Mount a generic volume provisioned/attached using an exec based plugin."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("flexVolume"),
				generateVolumeTypeList("flexVolume"),
				nil,
				nil,
			),
			"cinder": schema.NewPropertySchema(
				schema.NewRefSchema("CinderVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Cinder"),
					schema.PointerTo("Mount a cinder volume attached and mounted on the host machine."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("cinder"),
				generateVolumeTypeList("cinder"),
				nil,
				nil,
			),
			"cephfs": schema.NewPropertySchema(
				schema.NewRefSchema("CephFSVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("CephFS"),
					schema.PointerTo("Mount a CephFS volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("cephfs"),
				generateVolumeTypeList("cephfs"),
				nil,
				nil,
			),
			"flocker": schema.NewPropertySchema(
				schema.NewRefSchema("FlockerVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Flocker"),
					schema.PointerTo("Mount a Flocker volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("flocker"),
				generateVolumeTypeList("flocker"),
				nil,
				nil,
			),
			"downwardAPI": schema.NewPropertySchema(
				schema.NewRefSchema("DownwardAPIVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Downward API"),
					schema.PointerTo("Specify a volume that the pod should mount itself."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("downwardAPI"),
				generateVolumeTypeList("downwardAPI"),
				nil,
				nil,
			),
			"fc": schema.NewPropertySchema(
				schema.NewRefSchema("FCVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Fibre Channel"),
					schema.PointerTo("Mount a Fibre Channel volume that's attached to the host machine."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("fc"),
				generateVolumeTypeList("fc"),
				nil,
				nil,
			),
			"azureFile": schema.NewPropertySchema(
				schema.NewRefSchema("AzureFileVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Azure File"),
					schema.PointerTo("Mount an Azure File Service mount."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("azureFile"),
				generateVolumeTypeList("azureFile"),
				nil,
				nil,
			),
			"configMap": schema.NewPropertySchema(
				schema.NewRefSchema("ConfigMapVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("ConfigMap"),
					schema.PointerTo("Mount a ConfigMap as a volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("configMap"),
				generateVolumeTypeList("configMap"),
				nil,
				nil,
			),
			"vsphereVolume": schema.NewPropertySchema(
				schema.NewRefSchema("VsphereVirtualDiskVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("vSphere Virtual Disk"),
					schema.PointerTo("Mount a vSphere Virtual Disk as a volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("vsphereVolume"),
				generateVolumeTypeList("vsphereVolume"),
				nil,
				nil,
			),
			"quobyte": schema.NewPropertySchema(
				schema.NewRefSchema("QuobyteVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("quobyte"),
					schema.PointerTo("Mount Quobyte volume from the host."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("quobyte"),
				generateVolumeTypeList("quobyte"),
				nil,
				nil,
			),
			"azureDisk": schema.NewPropertySchema(
				schema.NewRefSchema("AzureDiskVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Azure Data Disk"),
					schema.PointerTo("Mount an Azure Data Disk as a volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("azureDisk"),
				generateVolumeTypeList("azureDisk"),
				nil,
				nil,
			),
			"photonPersistentDisk": schema.NewPropertySchema(
				schema.NewRefSchema("PhotonPersistentDiskVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("PhotonController persistent disk"),
					schema.PointerTo("Mount a PhotonController persistent disk as a volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("photonPersistentDisk"),
				generateVolumeTypeList("photonPersistentDisk"),
				nil,
				nil,
			),
			"projected": schema.NewPropertySchema(
				schema.NewRefSchema("ProjectedVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Projected"),
					schema.PointerTo("Projected items for all in one resources secrets, configmaps, and downward API."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("projected"),
				generateVolumeTypeList("projected"),
				nil,
				nil,
			),
			"portworxVolume": schema.NewPropertySchema(
				schema.NewRefSchema("PortworxVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Portworx Volume"),
					schema.PointerTo("Mount a Portworx volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("portworxVolume"),
				generateVolumeTypeList("portworxVolume"),
				nil,
				nil,
			),
			"scaleIO": schema.NewPropertySchema(
				schema.NewRefSchema("ScaleIOVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("ScaleIO Persistent Volume"),
					schema.PointerTo("Mount a ScaleIO persistent volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("scaleIO"),
				generateVolumeTypeList("scaleIO"),
				nil,
				nil,
			),
			"storageos": schema.NewPropertySchema(
				schema.NewRefSchema("StorageOSVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("StorageOS Volume"),
					schema.PointerTo("Mount a StorageOS volume."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("storageos"),
				generateVolumeTypeList("storageos"),
				nil,
				nil,
			),
			"csi": schema.NewPropertySchema(
				schema.NewRefSchema("CSIVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("CSI Volume"),
					schema.PointerTo("Mount a volume using a CSI driver."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("csi"),
				generateVolumeTypeList("csi"),
				nil,
				nil,
			),
			"ephemeral": schema.NewPropertySchema(
				schema.NewRefSchema("EphemeralVolumeSource", nil),
				schema.NewDisplayValue(
					schema.PointerTo("Ephemeral"),
					schema.PointerTo("Mount a volume that is handled by a cluster storage driver."),
					nil,
				),
				false,
				nil,
				generateVolumeTypeList("ephemeral"),
				generateVolumeTypeList("ephemeral"),
				nil,
				nil,
			),
		},
	),
	// endregion
	// region HostPathVolumeSource
	schema.NewStructMappedObjectSchema[v1.HostPathVolumeSource](
		"HostPathVolumeSource",
		map[string]*schema.PropertySchema{
			"path": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, nil),
				schema.NewDisplayValue(
					schema.PointerTo("Path"),
					schema.PointerTo("Path to the directory on the host."),
					nil,
				),
				true,
				nil,
				nil,
				nil,
				nil,
				[]string{`"/srv/volume1"`},
			),
			"type": schema.NewPropertySchema(
				schema.NewStringEnumSchema(
					map[string]*schema.DisplayValue{
						string(v1.HostPathUnset):             {NameValue: schema.PointerTo("Unset")},
						string(v1.HostPathDirectoryOrCreate): {NameValue: schema.PointerTo("Create directory if not found")},
						string(v1.HostPathDirectory):         {NameValue: schema.PointerTo("Directory")},
						string(v1.HostPathFileOrCreate):      {NameValue: schema.PointerTo("Create file if not found")},
						string(v1.HostPathFile):              {NameValue: schema.PointerTo("File")},
						string(v1.HostPathSocket):            {NameValue: schema.PointerTo("Socket")},
						string(v1.HostPathCharDev):           {NameValue: schema.PointerTo("Character device")},
						string(v1.HostPathBlockDev):          {NameValue: schema.PointerTo("Block device")},
					},
				),
				schema.NewDisplayValue(
					schema.PointerTo("Type"),
					schema.PointerTo("Type of the host path."),
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
	// endregion
	// region EmptyDirVolumeSource
	schema.NewStructMappedObjectSchema[v1.EmptyDirVolumeSource](
		"EmptyDirVolumeSource",
		map[string]*schema.PropertySchema{
			"medium": schema.NewPropertySchema(
				schema.NewStringSchema(schema.IntPointer(1), nil, regexp.MustCompile("^(|Memory|HugePages|HugePages-.*)$")),
				schema.NewDisplayValue(
					schema.PointerTo("Medium"),
					schema.PointerTo("How to store the empty directory"),
					nil,
				),
				false,
				nil,
				nil,
				nil,
				nil,
				nil,
			),
			//nolint:godox
			// TODO add sizeLimit option when Quantity is describable.
		},
	),
	// endregion
	// region GCEPersistentDiskVolumeSource
	schema.NewStructMappedObjectSchema[v1.GCEPersistentDiskVolumeSource](
		"GCEPersistentDiskVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region AWSElasticBlockStoreVolumeSource
	schema.NewStructMappedObjectSchema[v1.AWSElasticBlockStoreVolumeSource](
		"AWSElasticBlockStoreVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region SecretVolumeSource
	schema.NewStructMappedObjectSchema[v1.SecretVolumeSource](
		"SecretVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region NFSVolumeSource
	schema.NewStructMappedObjectSchema[v1.NFSVolumeSource](
		"NFSVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region ISCSIVolumeSource
	schema.NewStructMappedObjectSchema[v1.ISCSIVolumeSource](
		"ISCSIVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region GlusterfsVolumeSource
	schema.NewStructMappedObjectSchema[v1.GlusterfsVolumeSource](
		"GlusterfsVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region PersistentVolumeClaimVolumeSource
	schema.NewStructMappedObjectSchema[v1.PersistentVolumeClaimVolumeSource](
		"PersistentVolumeClaimVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region RBDVolumeSource
	schema.NewStructMappedObjectSchema[v1.RBDVolumeSource](
		"RBDVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region FlexVolumeSource
	schema.NewStructMappedObjectSchema[v1.FlexVolumeSource](
		"FlexVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region CinderVolumeSource
	schema.NewStructMappedObjectSchema[v1.CinderVolumeSource](
		"CinderVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region CephFSVolumeSource
	schema.NewStructMappedObjectSchema[v1.CephFSVolumeSource](
		"CephFSVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region FlockerVolumeSource
	schema.NewStructMappedObjectSchema[v1.FlockerVolumeSource](
		"FlockerVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region DownwardAPIVolumeSource
	schema.NewStructMappedObjectSchema[v1.DownwardAPIVolumeSource](
		"DownwardAPIVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region FCVolumeSource
	schema.NewStructMappedObjectSchema[v1.FCVolumeSource](
		"FCVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region AzureFileVolumeSource
	schema.NewStructMappedObjectSchema[v1.AzureFileVolumeSource](
		"AzureFileVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region ConfigMapVolumeSource
	schema.NewStructMappedObjectSchema[v1.ConfigMapVolumeSource](
		"ConfigMapVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region VsphereVirtualDiskVolumeSource
	schema.NewStructMappedObjectSchema[v1.VsphereVirtualDiskVolumeSource](
		"VsphereVirtualDiskVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region QuobyteVolumeSource
	schema.NewStructMappedObjectSchema[v1.QuobyteVolumeSource](
		"QuobyteVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region AzureDiskVolumeSource
	schema.NewStructMappedObjectSchema[v1.AzureDiskVolumeSource](
		"AzureDiskVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region PhotonPersistentDiskVolumeSource
	schema.NewStructMappedObjectSchema[v1.PhotonPersistentDiskVolumeSource](
		"PhotonPersistentDiskVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region ProjectedVolumeSource
	schema.NewStructMappedObjectSchema[v1.ProjectedVolumeSource](
		"ProjectedVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region PortworxVolumeSource
	schema.NewStructMappedObjectSchema[v1.PortworxVolumeSource](
		"PortworxVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region ScaleIOVolumeSource
	schema.NewStructMappedObjectSchema[v1.ScaleIOVolumeSource](
		"ScaleIOVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region StorageOSVolumeSource
	schema.NewStructMappedObjectSchema[v1.StorageOSVolumeSource](
		"StorageOSVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region CSIVolumeSource
	schema.NewStructMappedObjectSchema[v1.CSIVolumeSource](
		"CSIVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
	// region EphemeralVolumeSource
	schema.NewStructMappedObjectSchema[v1.EphemeralVolumeSource](
		"EphemeralVolumeSource",
		map[string]*schema.PropertySchema{},
	),
	// endregion
)

func generateVolumeTypeList(except string) []string {
	l := []string{
		"hostPath",
		"emptyDir",
		"gcePersistentDisk",
		"awsElasticBlockStore",
		"secret",
		"nfs",
		"iscsi",
		"glusterfs",
		"persistentVolumeClaim",
		"rbd",
		"flexVolume",
		"cinder",
		"cephfs",
		"flocker",
		"downwardAPI",
		"fc",
		"azureFile",
		"configMap",
		"vsphereVolume",
		"quobyte",
		"azureDisk",
		"photonPersistentDisk",
		"projected",
		"portworxVolume",
		"scaleIO",
		"storageos",
		"csi",
		"ephemeral",
	}
	var result []string
	for _, entry := range l {
		if entry != except {
			result = append(result, entry)
		}
	}
	return result
}

var identifier = schema.NewStringSchema(
	schema.IntPointer(1),
	nil,
	regexp.MustCompile(`^[a-zA-Z0-9-._]+$`),
)
var imageTag = schema.NewStringSchema(
	schema.IntPointer(1),
	nil,
	regexp.MustCompile(`^[a-zA-Z0-9_\-:./]+$`),
)
var labelName = schema.NewStringSchema(
	nil,
	nil,
	regexp.MustCompile(`^(|([a-zA-Z](|[a-zA-Z\-.]{0,251}[a-zA-Z0-9]))/)([a-zA-Z](|[a-zA-Z\\-]{0,61}[a-zA-Z0-9]))$`),
)
var labelValue = schema.NewStringSchema(
	nil,
	schema.IntPointer(63),
	regexp.MustCompile(`^(|[a-zA-Z0-9]+(|[-_.][a-zA-Z0-9]+)*[a-zA-Z0-9])$`),
)
var dnsSubdomainName = schema.NewStringSchema(
	nil,
	schema.IntPointer(253),
	regexp.MustCompile(`^[a-z0-9]($|[a-z0-9\-_]*[a-z0-9])$`),
)

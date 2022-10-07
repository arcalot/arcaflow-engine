package config

import (
	"go.arcalot.io/log"
	"go.flow.arcalot.io/pluginsdk/schema"
)

func getConfigSchema() *schema.TypedScopeSchema[*Config] { //nolint:funlen
	return schema.NewTypedScopeSchema[*Config](
		schema.NewStructMappedObjectSchema[*Config](
			"Config",
			map[string]*schema.PropertySchema{
				"log": schema.NewPropertySchema(
					schema.NewRefSchema("LogConfig", nil),
					schema.NewDisplayValue(
						schema.PointerTo("Logging"),
						schema.PointerTo("Logging configuration"),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"plugins": schema.NewPropertySchema(
					schema.NewListSchema(
						schema.NewStringSchema(schema.IntPointer(1), nil, nil),
						nil,
						nil,
					),
					schema.NewDisplayValue(
						schema.PointerTo("Plugins"),
						schema.PointerTo("Plugins to fetch schema from for JSON schema generation."),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				"deployer": schema.NewPropertySchema(
					schema.NewAnySchema(),
					schema.NewDisplayValue(
						schema.PointerTo("Local deployer"),
						schema.PointerTo(
							"Local container environment configuration the workflow engine can use to test-deploy plugins before the workflow execution.",
						),
						nil,
					),
					true,
					nil,
					nil,
					nil,
					schema.PointerTo("{\"type\":\"docker\"}"),
					nil,
				),
			},
		),
		schema.NewStructMappedObjectSchema[log.Config](
			"LogConfig",
			map[string]*schema.PropertySchema{
				"level": schema.NewPropertySchema(
					schema.NewStringEnumSchema(map[string]string{
						string(log.LevelDebug):   "Debug",
						string(log.LevelInfo):    "Informational",
						string(log.LevelWarning): "Warnings",
						string(log.LevelError):   "Errors",
					}),
					schema.NewDisplayValue(
						schema.PointerTo("Log level"),
						schema.PointerTo(
							"Minimum level of log messages to write.",
						),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					schema.PointerTo(`"info"`),
					nil,
				),
				"destination": schema.NewPropertySchema(
					schema.NewStringEnumSchema(map[string]string{
						string(log.DestinationStdout): "Standard output",
					}),
					schema.NewDisplayValue(
						schema.PointerTo("Log destination"),
						schema.PointerTo(
							"Where the logs should be written to.",
						),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					schema.PointerTo(`"stdout"`),
					nil,
				),
			},
		),
	)
}

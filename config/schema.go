package config

import (
	"regexp"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/util"
	"go.flow.arcalot.io/pluginsdk/schema"
)

func getConfigSchema() *schema.TypedScopeSchema[*Config] {
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
				"deployers": schema.NewPropertySchema(
					schema.NewRefSchema("LocalDeployers", nil),
					schema.NewDisplayValue(
						schema.PointerTo("Local deployers"),
						schema.PointerTo(
							"Default deployers for each plugin type.",
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
				"logged_outputs": schema.NewPropertySchema(
					schema.NewMapSchema(
						schema.NewStringSchema(
							schema.IntPointer(1),
							schema.IntPointer(255),
							regexp.MustCompile("^[$@a-zA-Z0-9-_]+$")),
						schema.NewRefSchema("StepOutputLogConfig", nil),
						nil,
						nil,
					),
					schema.NewDisplayValue(
						schema.PointerTo("Logged Outputs"),
						schema.PointerTo(
							"Step output types to log. Make sure output log level is equal to or greater than the minimum log value.",
						),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					schema.PointerTo("{}"),
					nil,
				),
			},
		),
		schema.NewStructMappedObjectSchema[*StepOutputLogConfig](
			"StepOutputLogConfig",
			map[string]*schema.PropertySchema{
				"level": schema.NewPropertySchema(
					schema.NewStringEnumSchema(map[string]*schema.DisplayValue{
						string(log.LevelDebug):   {NameValue: schema.PointerTo("Debug")},
						string(log.LevelInfo):    {NameValue: schema.PointerTo("Informational")},
						string(log.LevelWarning): {NameValue: schema.PointerTo("Warnings")},
						string(log.LevelError):   {NameValue: schema.PointerTo("Errors")},
					}),
					schema.NewDisplayValue(
						schema.PointerTo("Log level"),
						schema.PointerTo(
							"The level to log matching step outputs. Must be greater than the minimum log level.",
						),
						nil,
					),
					false,
					nil,
					nil,
					nil,
					schema.PointerTo(util.JSONEncode(log.LevelInfo)),
					nil,
				),
			},
		),
		schema.NewStructMappedObjectSchema[log.Config](
			"LogConfig",
			map[string]*schema.PropertySchema{
				"level": schema.NewPropertySchema(
					schema.NewStringEnumSchema(map[string]*schema.DisplayValue{
						string(log.LevelDebug):   {NameValue: schema.PointerTo("Debug")},
						string(log.LevelInfo):    {NameValue: schema.PointerTo("Informational")},
						string(log.LevelWarning): {NameValue: schema.PointerTo("Warnings")},
						string(log.LevelError):   {NameValue: schema.PointerTo("Errors")},
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
					schema.PointerTo(util.JSONEncode(log.LevelInfo)),
					nil,
				),
				"destination": schema.NewPropertySchema(
					schema.NewStringEnumSchema(map[string]*schema.DisplayValue{
						string(log.DestinationStdout): {NameValue: schema.PointerTo("Standard output")},
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
					schema.PointerTo(util.JSONEncode(log.DestinationStdout)),
					nil,
				),
			},
		),
		schema.NewStructMappedObjectSchema[LocalDeployers](
			"LocalDeployers",
			map[string]*schema.PropertySchema{
				"image": schema.NewPropertySchema(
					schema.NewAnySchema(),
					schema.NewDisplayValue(
						schema.PointerTo("Local Image deployer"),
						schema.PointerTo(
							"Local container environment configuration the workflow engine can use to test-deploy plugins before the workflow execution.",
						),
						nil,
					),
					//true,
					false,
					nil,
					nil,
					nil,
					schema.PointerTo("{\"type\":\"docker\"}"),
					//nil,
					nil,
				),
				"python": schema.NewPropertySchema(
					schema.NewAnySchema(),
					schema.NewDisplayValue(
						schema.PointerTo("Local Python deployer"),
						schema.PointerTo(
							"Local Python environment configuration the workflow engine can use to test-deploy plugins before the workflow execution.",
						),
						nil,
					),
					//true,
					false,
					nil,
					nil,
					nil,
					schema.PointerTo("{\"type\":\"python\"}"),
					//schema.PointerTo(`{"type": "python", "pythonPath": "/usr/bin/python"`),
					nil,
				),
			},
		),
	)
}

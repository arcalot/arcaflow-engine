package codegen

//go:generate go run gen/gen.go schema_input.yaml

func CodeGen() string {
	return "arcaflow-engine/codegen.CodeGen"
}

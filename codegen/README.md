# Arcaflow Engine golang code generator

The Arcaflow Engine golang code generator takes an Arcaflow schema YAML structure loaded using this library (see schema package) and generates the type definitions in Go code.

## Usage

Copy a valid Arcaflow schema to be used as input of the code generator into this folder and name it `schema_input.yaml`. Then run:

```
$ go generate
```

The output will be stored in the `output/typedef_output.go` file.
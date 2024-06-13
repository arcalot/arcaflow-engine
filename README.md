# Arcaflow Engine

The Arcaflow Engine allows you to run workflows using container engines, such as Docker,
Podman, or Kubernetes, or for supported plugins using Python directly. The Arcaflow SDK
is available for [Python](https://arcalot.io/arcaflow/creating-plugins/python/) and
[Golang](https://github.com/arcalot/arcaflow-plugin-sdk-go) to aid with the development
of plugins.

## Pre-built binaries

If you want to use our pre-built engine binaries, you can find them in the
[releases section](https://github.com/arcalot/arcaflow-engine/releases).

## Building from source

This system requires at least Go 1.18 to run and can be built from source:

```
go build -o arcaflow cmd/arcaflow/main.go
```

This binary can then be used to run Arcaflow workflows.

## Running a simple workflow

A set of [example workflows](https://github.com/arcalot/arcaflow-workflows) is available
to demonstrate workflow features. A basic example `workflow.yaml` may look like this:

```yaml
version: v0.2.0
input:
  root: RootObject
  objects:
    RootObject:
      id: RootObject
      properties:
        name:
          type:
            type_id: string
steps:
  example:
    plugin:
      deployment_type: image
      src: quay.io/arcalot/arcaflow-plugin-example
    input:
      name: !expr $.input.name
outputs:
  success:
    message: !expr $.steps.example.outputs.success.message
```

As you can see, a workflow has the root keys of `version`, `input`, `steps`, and
`outputs`. Each of these keys is required in a workflow. These can be linked together
using the Arcaflow
[expression language](https://arcalot.io/arcaflow/workflows/expressions/). The
expressions also determine the execution order of plugins.

An input file for this basic workflow may look like:

```yaml
name: Arca Lot
```

The Arcaflow engine uses a configuration to define the standard behaviors for deploying
plugins within the workflow. The default configuration will use Docker as the container
image deployer and will set the log outputs to the `info` level.

If you have a local Docker / Moby setup installed, you can run the workflow immediately:

```bash
arcaflow -input input.yaml
```

If you don't have a local Docker setup, or if you want to use another deployer or any
custom configuration parameters, you can create a `config.yaml` with your desired
parameters. For example:

```yaml
deployers:
  image: 
    deployer_name: podman
    deployment:
      host:
        NetworkMode: host
      imagePullPolicy: IfNotPresent
  python:
    deployer_name: python
log:
  level: debug
logged_outputs:
  error:
    level: debug
```

You can load this config by passing the `-config` flag to Arcaflow.

```bash
arcaflow -input input.yaml -config config.yaml
```

The default workflow file name is `workflow.yaml`, but you can override this with the
`-workflow` input parameter.

Arcaflow also accepts a `-context` parameter that defines the base directory for all
input files. All relative file paths are from the context directory, and absolute paths
are also supported. The default context is the current working directory (`.`).

### A few command examples...

Use the built-in configuration and run the `workflow.yaml` file from the `/my-workflow`
context directory with no input:
```bash
arcaflow -context /my-workflow
```

Use a custom `my-config.yaml` configuration file and run the `my-workflow.yaml` workflow
using the `my-input.yaml` input file from the current directory:
```bash
arcaflow -config my-config.yaml -workflow my-workflow.yaml -input my-input.yaml
```

Use a custom `config.yaml` configuration file and the default `workflow.yaml` file from
the `/my-workflow` context directory, and an `input.yaml` file from the current working
directory:
```bash
arcaflow -context /my-workflow -config config.yaml -input ./input.yaml
```

## Deployers

Image-based deployers are used to deploy plugins to container platforms. Each deployer
has configuraiton parameters specific to its platform. These deployers are:

- [Docker](https://github.com/arcalot/arcaflow-engine-deployer-docker)
- [Podman](https://github.com/arcalot/arcaflow-engine-deployer-podman)
- [Kubernetes](https://github.com/arcalot/arcaflow-engine-deployer-kubernetes)

There is also a
[Python deployer](https://github.com/arcalot/arcaflow-engine-deployer-python) that
allows for running supported plugins directly instead of containerized.

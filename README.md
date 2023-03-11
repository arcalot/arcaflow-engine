# Arcaflow Engine

The Arcaflow Engine allows you to run workflows using container engines, such as Docker or Kubernetes. The plugins must be built with the [Arcaflow SDK](https://arcalot.io/arcaflow/creating-plugins/python/).

## Pre-built binaries

If you want to use our pre-built binaries, you can find them in the [releases section](https://github.com/arcalot/arcaflow-engine/releases).

## Building from source

This system requires at least Go 1.18 to run and can be built from source:

```
go build -o arcaflow cmd/arcaflow/main.go
```

This binary can then be used to run Arcaflow workflows.

## Building a simple workflow

The simplest workflow is the example plugin workflow: (save it to workflow.yaml)

```yaml
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
    plugin: ghcr.io/janosdebugs/arcaflow-example-plugin
    # step: step-id if the plugin has more than one step
    # deploy:
    #   type: docker|kubernetes
    #   ... more options
    input:
      name: !expr $.input.name
output:
  message: !expr $.steps.example.outputs.success.message
```

As you can see, it has an input, a list of steps, and an output definition. These can be linked together using JSONPath expressions (not all features are supported). The expressions also determine the execution order of plugins.

You can now create an input YAML for this workflow: (save it to input.yaml)

```yaml
name: Arca Lot
```

If you have a local Docker / Moby setup installed, you can run it immediately:

```
./arcaflow -input input.yaml
```

If you don't have a local Docker setup, you can also create a `config.yaml` with the following structure:

```yaml
deployer:
  type: docker|kubernetes
  # More deployer options
log:
  level: debug|info|warning|error
```

You can load this config by passing the `-config` flag to Arcaflow.

## Deployer options

Currently, the two deployer types supported are Docker and Kubernetes.

### The Docker deployer

This deployer uses the Docker socket to launch containers. It has the following config structure:

```yaml
type: docker
connection:
  host: # Docker connection string
  cacert: # CA certificate for engine connection in PEM format
  cert: # Client cert in PEM format
  key: # Client key in PEM format
deployment:
  container: # Container options, see https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerCreate
  host: # Host options, see https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerCreate
  network: # Network options, see https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerCreate
  platform: # Platform options, see https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerCreate
  
  # Pull policy, similar to Kubernetes
  imagePullPolicy: Always|IfNotPresent|Never
timeouts:
  http: 15s
```

**Note:** not all container options are supported. STDIN/STDOUT-related options are disabled. Some other options may not be implemented yet, but you will always get an error message explaining missing options.

## The Kubernetes deployer

The Kubernetes deployer deploys on a Kubernetes cluster. It has the following config structure:

```yaml
type: kubernetes
connection:
  host: api.server.host
  path: /api
  username: foo
  password: bar
  serverName: tls.server.name
  cert: PEM-encoded certificate
  key: PEM-encoded key
  cacert: PEM-encoded CA certificate
  bearerToken: Bearer token for access
  qps: queries per second
  burst: burst value
deployment:
  metadata:
    # Add pod metadata here
  spec:
    # Add a normal pod spec here, plus the following option here:
    pluginContainer:
      # A single container configuration the plugin will run in. Do not specify the image, the engine will fill that.
timeouts:
  http: 15s
```

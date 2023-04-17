# Foreach step provider

This provider allows you to loop over a list of inputs and execute (potentially parallel) workflows for each item. The subworkflows must only have one possible output named "success" and this output will be collected into a list as a result.

## Usage

```yaml
steps:
  your_step:
    kind: foreach
    workflow: some_workflow_file.yaml # This must be in the workflow directory
    items: !expr $.input.some_list_of_items
    parallelism: 5 # How many workflows to run in parallel
output:
  result: !expr $.steps.your_step.outputs.success.data # This will be a list of result objects
```

### Handling errors

In case one or more subworkflows exit with an error, you can also recover.

```yaml
output:
  result: !expr $.steps.your_step.failed.error.data # This will be a map of int keys to provide the subworkflows with a successful execution.
  errors: !expr $.steps.your_step.failed.error.messages # This will be a map of int to error messages for the subworkflows that failed.
```
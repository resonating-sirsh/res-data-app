This example shows how to deploy multiple workflows/DAGs, and
then combine them into a larger dag.

### Docker
Here, we build a single Docker image with 2 scripts (hello_people and hello_world)
This gets built by Github Actions during CI

### Workflows
We have 3 workflows. `example-argo-hello-people.yaml` and `example-argo-hello-world.yaml` are basic 1-step workflows, but each one is a separate workflow template. These can be run in the Argo UI independently.

The final workflow is `example-argo-combined.yaml`, which references the first 2: it is a DAG, with the first 2 steps being the hello-people and hello-world steps. The 3rd step in the DAG is referencing another external template, deployed in a totally different example (apps/res-examples/example-argo-dag). This shows how you can create modular, reusable components and use them in different workflows.
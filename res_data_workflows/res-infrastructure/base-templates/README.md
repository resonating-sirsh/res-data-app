These templates are used by most other workflows. The can be imported in a step using e.g.:

    steps:
    - - name: myStep
        templateRef:
          name: default-res-data # This is the WorkflowTemplate, line 4 in default_res_data.yaml
          template: res-data # This is the template name inside the yaml file, line 13 in default_res_data.yaml

So far in this folder we have:

- an exit_handler, which will be used in all workflows to manage graceful and consistent exits
- a default res_data, which accepts a payload + a fully qualified function name from res/flows
    > for example, you would submit "examples.helloworld.hello_name" as the flow name, where the directory structure is this:
        res/
            flows/
                examples/
                    helloworld.py
    ... and inside helloworld.py we have a function named hello_name
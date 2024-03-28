# Example Workflow Callable

This workflow illustrates setting up a workflow that can be called. We simply omit the cron setting and use an example docker image
that can call a function on res-data. This function importantly takes an event payload which is assumed to be posted JSON. 

Using res-connect, we can call the workflow by template name. A default template called `flow-node` can be used to call functions without an /app but if you need either to create
a custom template or just manage your app versions in argo, then you can tell res-connect to run any template. Note it will use the docker image associated with the build


```python 
#TODO double check convention for workflow names
template_name  = "example-workflow-callable-workflow"
url = f"https://datadev.resmagic.io/res-connect/flows/{template_name}"

event = {"some" : "payload"}

#POST YOUR DATA
```



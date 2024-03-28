res_data_workflows contains Argo workflows that are more complex than the single-container flows
used in the apps/ folder. Currently all workflows in here use the res-data image (based on the Dockerfile in the root of this repo).

There are folders for namespaces, within which we have workflows:

/namespace
    /appname
        kustomize.yaml
        argoKinds.yaml
        *.yaml

To create a new app, you need to copy in the `kustomize.yaml` and `argoKinds.yaml` files into your new app's folder. Inside `kustomization` you need to add in any files that reference the res-data Docker image. In your yaml files, you can refer to the res-data Docker image like:

```image: res-data```

During the github Actions CI process, res-data will be swapped out for the actual repo with an up to date tag. You will see the updated tag once you deploy this to ArgoCD/Argo.


PLEASE NOTE:
- your Yaml files can be named anything, and will not affect naming in ArgoCD/Argo
- you should use `argo lint` to validate your yaml files to avoid confusion during CI/CD
- The "project" in ArgoCD will be named "workflow-{}".format(namespace)
- The appname folder will be the "app" name in ArgoCD... but the _workflow name_ in Argo (not ArgoCD) is defined in your yaml
- The namespace the actual workflow lives in is always "argo"






todos:

- inject all base env vars

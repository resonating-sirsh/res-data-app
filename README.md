# res-data-platform

## Todo

- check for changed files before the black etc
- run tests on the docker image in low code
- find a way to do DRY on the existing yaml to make it simpler
- kick off the infra repo PR and close it when our PR closes. That will be a simple ephemeral app
- scale down all the dev things to 0 for now - these can become staging apps
- confirm all the tags
- have the infra app deploy to master for some test apps

```
docker buildx build -t 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:latest \
--cache-to mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache \
--cache-from type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache ../../../

docker push <account-id>.dkr.ecr.<my-region>.amazonaws.com/buildkit-test:image
```

## Changes

We change how the base image is used in Apps so this is required for migration. Use the image:{sha} only from the build

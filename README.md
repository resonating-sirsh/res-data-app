# res-data-platform

hiasdfsdfsdf

```
docker buildx build -t 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:latest \
--cache-to mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache \
--cache-from type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache ../../../

docker push <account-id>.dkr.ecr.<my-region>.amazonaws.com/buildkit-test:image
```

## Changes

We change how the base image is used in Apps so this is required for migration. Use the image:{sha} only from the build

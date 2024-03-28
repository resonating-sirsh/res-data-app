# res-data-platform

```
docker build -t 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:latest \
--cache-to mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache \
--cache-from type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache ../../../

docker push <account-id>.dkr.ecr.<my-region>.amazonaws.com/buildkit-test:image
```

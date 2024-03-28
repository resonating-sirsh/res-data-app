This is just a REST api for signing a s3 url so that we can make objects publicly accessible with an expiration.

To build locally (do all this from the repo root i.e., res-data-platform/):
```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
docker build --pull --rm --build-arg RES_ENV=development --tag s3signer -f apps/res-create/s3url-signer/Dockerfile . 
```

To run locally in docker:
```
docker run -e COGNITO_USERPOOL_ID=userpool -e AWS_ACCESS_KEY=[stuff] -e AWS_SECRET_KEY=[stuff] -p5000:5000 s3signer
```
#!/bin/bash

set -e

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com

docker run\
       -it\
       --rm\
       --entrypoint bash\
       --hostname=localhost\
       --platform linux/amd64\
       -e AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)\
       -e AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)\
       -e AWS_DEFAULT_REGION=us-east-1\
       -e RES_ENV=development\
       286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-development:latest

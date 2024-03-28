#!/bin/bash

if [ -z "$1" ]
  then
    echo "No argument supplied. You must specify a tag even if its [latest]"
    exit 1
fi

echo "Building docker tag: $1"

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
docker build --pull --rm -f "./res-data/Dockerfile" -t res-data-development:$1 --platform linux/amd64 "../../" 
docker tag res-data-development:$1 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-development:$1
docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-development:$1

# docker build --pull --rm -f "Dockerfile" -t res-data:latest --platform linux/amd64 "." 
# docker tag res-data:latest286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data:latest & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data:latest

FROM rayproject/ray:2.9.0-py310

WORKDIR /serve_app

#Using the res requirements - the basic ones
COPY ./res/docker/res-ray/requirements.txt /serve_app/res/

######################################   PIP     ###################################################
#we can add the below or we can base of a lib docker image WIP
#RUN pip install --no-cache-dir torch==2.0.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install -U pip && pip install --no-cache-dir -r /serve_app/res/requirements.txt 

COPY ./res /serve_app/res/
COPY ./res_tests /serve_app/res_tests/
COPY ./res-schemas /serve_app/res-schemas/
COPY ./resources /serve_app/resources/
COPY ./apps/res-ray/query_server/  /serve_app/query_server 

ADD  ./res-schemas/pydantic/ /serve_app/schemas/pydantic/

ENV PYTHONPATH "${PYTHONPATH}:/serve_app"
ENV PYTHONUNBUFFERED=0

##aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
#docker build --pull --rm -f "Dockerfile" -t res-ray:latest --platform linux/amd64 ../../../
#docker tag res-ray:latest 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:latest & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray:latest

#helm install raycluster kuberay/ray-cluster --set image.tag=latest --set image.repository=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-ray 
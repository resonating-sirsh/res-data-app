ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/klaviyo_data_integration/ /app/

RUN pip install --force-reinstall -r /app/requirements.txt

ENV RES_TEAM "iamcurious"
ENV AM_I_IN_A_DOCKER_CONTAINER "true"
ENV PYTHONPATH "${PYTHONPATH}:/app/"

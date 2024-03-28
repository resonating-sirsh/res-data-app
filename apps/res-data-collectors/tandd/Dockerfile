ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/tandd/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM "iamcurious"
ENTRYPOINT ["python", "/app/main.py"]

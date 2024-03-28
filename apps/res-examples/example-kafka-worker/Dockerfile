ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-examples/example-kafka-worker/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/main.py"]

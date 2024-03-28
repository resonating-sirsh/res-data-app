ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-infrastructure/notification_from_kafka_to_hasura/ /app/

RUN pip install -r ./requirements.txt
# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0
CMD ["python", "/app/src/main.py"]
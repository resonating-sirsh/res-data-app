ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

ADD ./apps/res-infrastructure/kafka_to_airtable/ /app/
#RUN pip install -r requirements.txt

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "/app/src/main.py"]
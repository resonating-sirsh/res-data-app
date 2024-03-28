ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/quickbooks/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "/app/src/main.py"]
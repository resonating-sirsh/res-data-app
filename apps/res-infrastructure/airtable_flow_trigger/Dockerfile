ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

ADD ./apps/res-infrastructure/airtable_flow_trigger/ /app/
ENV PYTHONPATH "${PYTHONPATH}:/app/"

CMD ["python", "/app/src/main.py"]
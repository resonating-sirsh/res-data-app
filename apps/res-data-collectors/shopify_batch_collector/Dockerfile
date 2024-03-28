ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/shopify_batch_collector/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"

CMD ["python", "/app/src/main.py"]
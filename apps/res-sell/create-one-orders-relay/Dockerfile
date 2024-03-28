ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

ADD ./apps/res-sell/create-one-orders-relay/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "/app/src/main.py"]

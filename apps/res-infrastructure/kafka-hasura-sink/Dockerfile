ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

RUN pip install "pydantic>=1.10.2,<2" "stringcase"
ADD ./apps/res-infrastructure/kafka-hasura-sink/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/


ENV RES_TEAM "res-infrastructure"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/src/main.py"]

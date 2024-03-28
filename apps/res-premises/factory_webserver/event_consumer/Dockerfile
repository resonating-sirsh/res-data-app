ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

ADD ./apps/res-premises/event_consumer/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/main.py"]

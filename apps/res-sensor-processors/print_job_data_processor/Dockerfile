ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

ADD ./apps/res-sensor-processors/print_job_data_processor/ /app/

ENV PYTHONUNBUFFERED=0
ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENV RES_TEAM "res-premises"

CMD ["python", "/app/src/main.py"]

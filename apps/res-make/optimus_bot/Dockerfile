ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

RUN pip install apscheduler

ADD ./apps/res-make/optimus_bot/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
CMD ["python", "/app/main.py"]

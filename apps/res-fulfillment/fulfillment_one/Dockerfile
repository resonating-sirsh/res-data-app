ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

ADD ./apps/res-fulfillment/fulfillment_one/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

RUN pip install \
    "fastapi>=0.85.0,<1" \
    "pydantic[email]>=1.10.2,<2" \
    "hypercorn" \
    "uvicorn" \
    "stringcase"

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
#https://fastapi.tiangolo.com/deployment/docker/
CMD ["hypercorn", "--bind", "0.0.0.0:5000", "main:app"]


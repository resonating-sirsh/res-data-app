ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data_res:${RES_TAG} 

WORKDIR /app

RUN pip install \
    "fastapi>=0.85.0,<1" \
    "pydantic[email]>=1.10.2,<2" \
    "hypercorn" \
    "uvicorn" \
    "stringcase" \
    "fastapi-utils==0.2.1" \
    "fastapi-cognito==2.3.0" \
    "httpx"

ADD ./apps/res-meta/meta_one/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
#https://fastapi.tiangolo.com/deployment/docker/
CMD ["hypercorn", "--bind", "0.0.0.0:5000", "main:app"]


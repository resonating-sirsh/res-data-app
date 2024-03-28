ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

WORKDIR /app

ADD ./apps/res-infrastructure/res_connect/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

RUN pip install flask-cors

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 5000
CMD ["python", "/app/main.py"]

#aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
#docker build --pull --rm -f "Dockerfile" -t res-connect:latest --platform linux/amd64 "../../../"  
#docker run  -e  RES_ENV='testing' -t  docker.io/library/res-connect:latest pytest -m "not (data or slow or service or media)"

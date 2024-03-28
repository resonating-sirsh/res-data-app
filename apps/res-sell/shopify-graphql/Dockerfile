ARG RES_ENV
ARG RES_TAG
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:${RES_TAG:-latest}

RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev

WORKDIR /app
# Add requirements first so we don't have to reinstall each build
ADD ./apps/res-sell/shopify-graphql/requirements.txt /app/
RUN pip install -r requirements.txt
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

# Add in app code and libraries
ADD ./apps/res-sell/shopify-graphql/ /app/ 

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM "res-sell"
ENV PYTHONUNBUFFERED=0

CMD ["python", "/app/src/main.py"]
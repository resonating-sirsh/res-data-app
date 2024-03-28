FROM python:3.8

RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev
RUN mkdir /app
WORKDIR /app
# Add requirements first so we don't have to reinstall each build
ADD ./apps/res-sell/shopify-product-graphql/requirements.txt /app/
RUN pip install -r requirements.txt

# Add in app code and libraries
RUN mkdir /app/res
ADD ./res /app/res
ADD ./apps/res-sell/shopify-product-graphql/ /app/

# Set python path to discover files
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM = "res-sell"
ENV PYTHONUNBUFFERED=0

CMD ["python", "/app/src/main.py"]
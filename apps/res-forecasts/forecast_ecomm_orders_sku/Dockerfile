ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

RUN mkdir -p /app
ADD ./apps/res-forecasts/forecast_ecomm_orders_sku/ /app/

RUN pip install pyairtable==1.0.0.post1

ENV RES_TEAM "iamcurious"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/src/main.py"]

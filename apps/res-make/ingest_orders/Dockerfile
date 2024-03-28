FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-media-base:latest
WORKDIR /app/
COPY res/docker/res-data/requirements.txt /app

RUN pip install -U pip && pip install --no-cache-dir -r /app/requirements.txt
COPY res /app/res/
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0
#...for testing take straight from the etl
ENTRYPOINT ["python", "-m", "res",  "flow", "run", "etl.orders.shopify_to_make_orders"]

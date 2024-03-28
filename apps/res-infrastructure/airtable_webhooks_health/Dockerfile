#todo replace this with res minimal docker base
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-media-base:latest
WORKDIR /app
COPY res/docker/res-data/requirements.txt /app
RUN pip install -r requirements.txt
ADD ./apps/res-infrastructure/airtable_webhooks_health/ /app/
COPY res /app/res/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

ENTRYPOINT ["python", "/app/main.py"]


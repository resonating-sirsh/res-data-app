FROM python:3.8

RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev
RUN mkdir /app
WORKDIR /app
ADD ./apps/res-infrastructure/airtable_alerts/requirements.txt /app/
RUN pip install -r requirements.txt

RUN mkdir /app/res
ADD ./res /app/res
ADD ./apps/res-infrastructure/airtable_alerts/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=1

EXPOSE 5000
CMD ["python", "/app/src/main.py"]
FROM python:3.8

RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev
RUN mkdir /app
WORKDIR /app
# Add requirements first so we don't have to reinstall each build
ADD ./apps/res-infrastructure/res_notifications/requirements.txt /app/
RUN pip install -r requirements.txt

RUN mkdir /app/res
ADD ./res /app/res
ADD ./apps/res-infrastructure/res_notifications/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "/app/main.py"]
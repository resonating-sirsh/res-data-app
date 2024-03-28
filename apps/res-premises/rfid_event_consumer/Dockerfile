ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-premises/rfid_event_consumer/ /app/
ADD ./schemas/ /app/schemas/
ADD ./res/ /app/res/
RUN pip install pyairtable
RUN pip install --upgrade pyairtable
RUN pip install slack_sdk

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/main.py"]

ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest


COPY res/docker/res-data/requirements.txt /app
RUN pip install -r requirements.txt

ADD ./apps/res-sell/sbc-swap-requests/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/src/main.py"]

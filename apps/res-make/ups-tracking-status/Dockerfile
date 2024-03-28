ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-make/ups-tracking-status/ /app/

RUN pip install --upgrade pip setuptools && \
    pip install -r requirements.txt

ENV RES_TEAM="optimus"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "-m", "tracking_status.main"]





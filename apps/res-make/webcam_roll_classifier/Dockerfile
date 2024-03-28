ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

ADD ./apps/res-make/webcam_roll_classifier/ /app/

ENV RES_TEAM "res-make"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/src/main.py"]

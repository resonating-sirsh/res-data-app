ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-make/airtable_nest_evaluations_sink/ /app/

RUN pip install pyairtable==1.0.0.post1

ENV RES_TEAM "res-make"
ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/src/main.py"]
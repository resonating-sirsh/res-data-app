ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-premises/copy_print_job_data/ /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM "res-premises"

ENTRYPOINT ["python", "/app/src/main.py"]
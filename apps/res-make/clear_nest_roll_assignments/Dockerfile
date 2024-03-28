ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-$RES_ENV:latest

COPY ./apps/res-make/clear_nest_roll_assignments/src /app/src

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENV RES_TEAM "res-make"
ENTRYPOINT ["python", "/app/src/main.py"]

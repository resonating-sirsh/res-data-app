ARG RES_ENV="production"
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

WORKDIR /app/

ADD ./apps/res-make/clear_records/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY apps/res-make/clear_records/src /app/src
COPY res /app/res/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV RES_TEAM="optimus"

ENV PYTHONPATH "${PYTHONPATH}:/app/"


ENTRYPOINT ["python", "src/clear_print_records.py"]
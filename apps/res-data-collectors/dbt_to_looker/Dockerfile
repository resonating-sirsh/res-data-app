ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

ADD ./apps/res-data-collectors/dbt_to_looker/ /app/
RUN pip install --force-reinstall -r /app/requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app/"
ENTRYPOINT ["python", "/app/main.py"]
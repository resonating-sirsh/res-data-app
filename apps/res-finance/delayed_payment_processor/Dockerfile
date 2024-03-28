ARG RES_ENV
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-lite-$RES_ENV:latest

RUN apt-get update && apt-get install

ADD ./apps/res-finance/delayed_payment_processor/ /app/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/

RUN pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app/"

ENTRYPOINT ["python", "/app/src/main.py"] 